import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import docker
import paramiko
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteEntities

import latch_cli.tinyrequests as tinyrequests
from latch_cli.centromere.utils import (
    construct_dkr_client,
    construct_ssh_client,
    import_flyte_objects,
)
from latch_cli.config.latch import LatchConfig
from latch_cli.utils import (
    account_id_from_token,
    current_workspace,
    generate_temporary_ssh_credentials,
    hash_directory,
    retrieve_or_login,
)

config = LatchConfig()
endpoints = config.sdk_endpoints


@dataclass
class Container:
    dockerfile: Path
    image_name: str


class CentromereCtx:
    """Manages state for interaction with centromere.

    The context holds values that are relevant throughout the "lifetime" of a
    registration or remote execution, eg. location of local code and
    package name, as well as managing docker, ssh clients.
    """

    dkr_repo: Optional[str] = None
    dkr_client: docker.APIClient = None
    ssh_client: paramiko.client.SSHClient = None
    pkg_root: Path = None  # root
    ssh_key_path: Path = None
    disable_auto_version: bool = False
    image_full = None
    token = None
    version = None
    serialize_dir = None
    default_container: Container
    # Used to asscociate alternate containers with tasks
    container_map: Dict[str, Container] = {}

    latch_register_api_url = endpoints["register-workflow"]
    latch_image_api_url = endpoints["initiate-image-upload"]
    latch_provision_url = endpoints["provision-centromere"]

    def __init__(
        self,
        pkg_root: Path,
        token: Optional[str] = None,
        disable_auto_version: bool = False,
        remote: bool = False,
    ):
        try:
            if token is None:
                self.token = retrieve_or_login()
            else:
                self.token = token

            ws = current_workspace()
            if ws == "" or ws is None:
                self.account_id = account_id_from_token(self.token)
            else:
                self.account_id = ws

            self.pkg_root = Path(pkg_root).resolve()
            self.disable_auto_version = disable_auto_version
            try:
                version_file = self.pkg_root.joinpath("version")
                with open(version_file, "r") as vf:
                    self.version = vf.read().strip()
                if not self.disable_auto_version:
                    hash = hash_directory(self.pkg_root)
                    self.version = self.version + "-" + hash[:6]
            except Exception as e:
                raise ValueError(
                    f"Unable to extract pkg version from {str(self.pkg_root)}"
                ) from e

            self.dkr_repo = LatchConfig.dkr_repo
            self.remote = remote

            default_dockerfile = self.pkg_root.joinpath("Dockerfile")
            if not default_dockerfile.exists():
                raise FileNotFoundError(
                    "Make sure you are passing a directory that contains a ",
                    "valid dockerfile to '$latch register'.",
                )

            self.default_container = Container(
                dockerfile=default_dockerfile, image_name=self.image_tagged
            )

            import_flyte_objects([self.pkg_root])
            # Global FlyteEntities object holds all serializable objects after they are imported.
            for entity in FlyteEntities.entities:
                if isinstance(entity, PythonTask):
                    if (
                        hasattr(entity, "dockerfile_path")
                        and entity.dockerfile_path is not None
                    ):
                        self.container_map[entity.name] = Container(
                            dockerfile=entity.dockerfile_path,
                            image_name=self.task_image_name(entity.name),
                        )

            if remote is True:
                public_ip, username = self.nucleus_provision_url()

                self.dkr_client = construct_dkr_client(
                    ssh_host=f"ssh://{username}@{public_ip}"
                )
                self.ssh_client = construct_ssh_client(
                    host_ip=public_ip,
                    username=username,
                )
            else:
                self.dkr_client = construct_dkr_client()
        except Exception as e:
            self.cleanup()
            raise e

    @property
    def image(self):
        """The image to be registered."""
        if self.account_id is None:
            raise ValueError("You need to log in before you can register a workflow.")

        # CAUTION ~ this weird formatting is maintained indepedently in the
        # nucleus endpoint and here.
        # Name for federated token request has minimum of 2 characters.
        if int(self.account_id) < 10:
            account_id = f"x{self.account_id}"
        else:
            account_id = self.account_id

        return f"{account_id}_{self.pkg_root.name}"

    @property
    def image_tagged(self):
        """The tagged image to be registered.

        eg. dkr.ecr.us-west-2.amazonaws.com/pkg_name:version
        """
        if self.version is None:
            raise ValueError(
                "Attempting to create a tagged image name without first "
                "extracting the package version."
            )

        # From AWS:
        #   A tag name must be valid ASCII and may contain lowercase and uppercase letters,
        #   digits, underscores, periods and dashes. A tag name may not start with a period
        #   or a dash and may contain a maximum of 128 characters.

        match = re.match("^[a-zA-Z0-9_][a-zA-Z0-9._-]{,127}", self.version)
        if not match or match.span()[0] != 0 or match.span()[1] != len(self.version):
            raise ValueError(
                f"{self.version} is an invalid version for AWS "
                "ECR. Please provide a version that accomodates the ",
                "tag restrictions listed here - ",
                "https://docs.aws.amazon.com/AmazonECR/latest/userguide/ecr-using-tags.html",
            )

        if self.image is None or self.version is None:
            raise ValueError(
                "Attempting to create a tagged image name without first "
                " logging in or extracting the package version."
            )
        return f"{self.image}:{self.version}"

    def task_image_name(self, task_name: str) -> str:
        return f"{self.image}:{task_name}-{self.version}"

    @property
    def full_image(self):
        """The full image to be registered (without a tag).

            <repo/image>


        An example: ::

            dkr.ecr.us-west-2.amazonaws.com/pkg_name

        """
        return f"{self.dkr_repo}/{self.image}"

    def nucleus_provision_url(self) -> (str, str):
        """Retrieve centromere IP + username."""

        headers = {"Authorization": f"Bearer {self.token}"}

        self.ssh_key_path = Path(self.pkg_root) / ".ssh_key"
        public_key = generate_temporary_ssh_credentials(self.ssh_key_path)

        response = tinyrequests.post(
            self.latch_provision_url,
            headers=headers,
            json={
                "public_key": public_key,
            },
        )

        resp = response.json()
        try:
            public_ip = resp["ip"]
            username = resp["username"]
        except KeyError as e:
            raise ValueError(
                f"Malformed response from request for access token {resp}"
            ) from e
        return public_ip, username

    # utils for context management

    def __enter__(self):
        return self

    def cleanup(self):
        if self.ssh_key_path is not None:
            cmd = ["ssh-add", "-d", self.ssh_key_path]
            subprocess.run(cmd)
            self.ssh_key_path.unlink(missing_ok=True)
            self.ssh_key_path.with_suffix(".pub").unlink(missing_ok=True)

    def __exit__(self, type, value, traceback):
        self.cleanup()