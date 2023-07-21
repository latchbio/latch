import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import docker
import paramiko
import paramiko.util
from docker.transport import SSHHTTPAdapter
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteEntities
from flytekit.core.workflow import PythonFunctionWorkflow
from latch_sdk_config.latch import config

import latch_cli.tinyrequests as tinyrequests
from latch_cli.centromere.utils import (
    RemoteConnInfo,
    _construct_dkr_client,
    _construct_ssh_client,
    _import_flyte_objects,
)
from latch_cli.constants import latch_constants
from latch_cli.docker_utils import generate_dockerfile
from latch_cli.utils import (
    account_id_from_token,
    current_workspace,
    generate_temporary_ssh_credentials,
    hash_directory,
    retrieve_or_login,
)


@dataclass
class _Container:
    dockerfile: Path
    pkg_dir: Path
    image_name: str


class _CentromereCtx:
    """Manages state for interaction with centromere.

    The context holds values that are relevant throughout the "lifetime" of a
    registration or remote execution, eg. location of local code and
    package name, as well as managing docker, ssh clients.
    """

    dkr_repo: Optional[str] = None
    dkr_client: Optional[docker.APIClient] = None
    ssh_client: Optional[paramiko.SSHClient] = None
    pkg_root: Optional[Path] = None  # root
    disable_auto_version: bool = False
    image_full = None
    token = None
    version = None
    serialize_dir = None
    default_container: _Container
    # Used to associate alternate containers with tasks
    container_map: Dict[str, _Container]
    workflow_name: Optional[str]

    latch_register_api_url = config.api.workflow.register
    latch_image_api_url = config.api.workflow.upload_image
    latch_provision_url = config.api.centromere.provision
    latch_get_image_url = config.api.workflow.get_image
    latch_check_version_url = config.api.workflow.check_version

    ssh_key_path: Optional[Path] = None
    jump_key_path: Optional[Path] = None
    ssh_config_path: Optional[Path] = None

    internal_ip: Optional[str] = None
    username: Optional[str] = None

    def __init__(
        self,
        pkg_root: Path,
        token: Optional[str] = None,
        disable_auto_version: bool = False,
        remote: bool = False,
        *,
        use_new_centromere: bool = False,
    ):
        self.use_new_centromere = use_new_centromere

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
            self.dkr_repo = config.dkr_repo
            self.remote = remote
            self.container_map = {}

            default_dockerfile = self.pkg_root.joinpath("Dockerfile")
            if not default_dockerfile.exists():
                generate_dockerfile(
                    self.pkg_root, self.pkg_root.joinpath(".latch/Dockerfile")
                )
                default_dockerfile = self.pkg_root.joinpath(".latch/Dockerfile")

            _import_flyte_objects([self.pkg_root])

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

            # Global FlyteEntities object holds all serializable objects after they are imported.
            for entity in FlyteEntities.entities:
                if isinstance(entity, PythonFunctionWorkflow):
                    self.workflow_name = entity.name
                if isinstance(entity, PythonTask):
                    if (
                        hasattr(entity, "dockerfile_path")
                        and entity.dockerfile_path is not None
                    ):
                        self.container_map[entity.name] = _Container(
                            dockerfile=entity.dockerfile_path,
                            image_name=self.task_image_name(entity.name),
                            pkg_dir=entity.dockerfile_path.parent,
                        )

            if self.nucleus_check_version(self.version, self.workflow_name) is True:
                raise ValueError(f"Version {self.version} has already been registered.")

            self.default_container = _Container(
                dockerfile=default_dockerfile,
                image_name=self.image_tagged,
                pkg_dir=self.pkg_root,
            )

            if remote is True:
                self.ssh_key_path = Path(self.pkg_root) / latch_constants.pkg_ssh_key
                self.jump_key_path = Path(self.pkg_root) / latch_constants.pkg_jump_key
                self.public_key = generate_temporary_ssh_credentials(self.ssh_key_path)

                if use_new_centromere:
                    self.internal_ip, self.username = (
                        self.provision_register_deployment()
                    )
                else:
                    self.internal_ip, self.username = self.get_old_centromere_info()

                remote_conn_info = RemoteConnInfo(
                    ip=self.internal_ip,
                    username=self.username,
                    jump_key_path=self.jump_key_path,
                    ssh_key_path=self.ssh_key_path,
                )

                ssh_client = _construct_ssh_client(
                    remote_conn_info, use_gateway=use_new_centromere
                )
                self.ssh_client = ssh_client

                def _patched_connect(self):
                    ...

                def _patched_create_paramiko_client(self, base_url):
                    self.ssh_client = ssh_client

                SSHHTTPAdapter._create_paramiko_client = _patched_create_paramiko_client
                SSHHTTPAdapter._connect = _patched_connect

                self.dkr_client = _construct_dkr_client(ssh_host="ssh://fake")

            else:
                self.dkr_client = _construct_dkr_client()
        except (Exception, KeyboardInterrupt) as e:
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

        match = re.match("^[a-zA-Z0-9_][a-zA-Z0-9._-]{,127}$", self.version)
        if match is None:
            raise ValueError(
                f"{self.version} is an invalid version for AWS "
                "ECR. Please provide a version that accomodates the "
                "tag restrictions listed here - "
                "https://docs.aws.amazon.com/AmazonECR/latest/userguide/ecr-using-tags.html"
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

    def get_old_centromere_info(self) -> Tuple[str, str]:
        headers = {"Authorization": f"Bearer {self.token}"}

        response = tinyrequests.post(
            self.latch_provision_url,
            headers=headers,
            json={
                "public_key": self.public_key,
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

    def provision_register_deployment(self) -> Tuple[str, str]:
        """Retrieve centromere IP + username."""
        print("Provisioning register instance. This may take a few minutes.")

        resp = tinyrequests.post(
            "https://centromere.latch.bio/register/start",
            headers={"Authorization": f"Latch-SDK-Token {self.token}"},
            json={"SSHKey": self.ssh_key_path.with_suffix(".pub").read_text()},
        )

        json_data = resp.json()
        if resp.status_code != 200:
            raise ValueError(json_data["Error"])

        hostname = json_data["InternalHost"]
        self.jump_key_path.write_text(json_data["JumpKey"])
        self.jump_key_path.chmod(0o600)

        self.centromere_hostname = hostname

        return hostname, "root"

    def downscale_register_deployment(self):
        if not (self.remote and self.use_new_centromere):
            return

        resp = tinyrequests.post(
            "https://centromere.latch.bio/register/stop",
            headers={"Authorization": f"Latch-SDK-Token {self.token}"},
            json={"InternalHostName": self.centromere_hostname},
        )

        if resp.status_code != 200:
            raise ValueError("unable to downscale register deployment")

    def nucleus_get_image(self, task_name: str, version: Optional[str] = None) -> str:
        """Retrieve fqn of the container for a task and optional version."""

        headers = {"Authorization": f"Bearer {self.token}"}
        response = tinyrequests.post(
            self.latch_get_image_url,
            headers=headers,
            json={
                "task_name": task_name,
            },
        )

        resp = response.json()
        try:
            return resp["image_name"]
        except KeyError as e:
            raise ValueError(
                f"Malformed response from request for access token {resp}"
            ) from e

    def nucleus_check_version(self, version: str, workflow_name: str) -> bool:
        """Check if version has already been registered for given workflow"""

        headers = {"Authorization": f"Bearer {self.token}"}

        ws_id = current_workspace()
        if ws_id is None or ws_id == "":
            ws_id = account_id_from_token(retrieve_or_login())

        response = tinyrequests.post(
            self.latch_check_version_url,
            headers=headers,
            json={
                "version": version,
                "workflow_name": workflow_name,
                "ws_account_id": ws_id,
            },
        )

        resp = response.json()
        try:
            return resp["exists"]
        except KeyError as e:
            raise ValueError(
                f"Malformed response from request for access token {resp}"
            ) from e

    def __enter__(self):
        return self

    def cleanup(self):
        if self.ssh_key_path is not None:
            self.ssh_key_path.unlink(missing_ok=True)
            self.ssh_key_path.with_suffix(".pub").unlink(missing_ok=True)
        if self.jump_key_path is not None:
            self.jump_key_path.unlink(missing_ok=True)

        self.downscale_register_deployment()

    def __exit__(self, type, value, traceback):
        self.cleanup()
