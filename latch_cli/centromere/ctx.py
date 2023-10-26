import re
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional, Tuple

import click
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
from latch_cli.constants import docker_image_name_illegal_pat
from latch_cli.docker_utils import get_default_dockerfile
from latch_cli.utils import (
    WorkflowType,
    account_id_from_token,
    current_workspace,
    generate_temporary_ssh_credentials,
    hash_directory,
    retrieve_or_login,
)

from ..services.register.utils import import_module_by_path
from ..utils import identifier_suffix_from_str


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
    version = None
    serialize_dir = None
    default_container: _Container
    workflow_type: WorkflowType
    snakefile: Optional[Path]

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
        *,
        disable_auto_version: bool = False,
        remote: bool = False,
        snakefile: Optional[Path] = None,
        use_new_centromere: bool = False,
    ):
        self.use_new_centromere = use_new_centromere
        self.remote = remote
        self.disable_auto_version = disable_auto_version

        try:
            self.token = retrieve_or_login()
            self.account_id = current_workspace()

            self.dkr_repo = config.dkr_repo
            self.pkg_root = pkg_root.resolve()

            if snakefile is None:
                self.workflow_type = WorkflowType.latchbiosdk
            else:
                self.workflow_type = WorkflowType.snakemake
                self.snakefile = snakefile

            self.container_map: Dict[str, _Container] = {}
            if self.workflow_type == WorkflowType.latchbiosdk:
                _import_flyte_objects([self.pkg_root])
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
                if not hasattr(self, "workflow_name"):
                    click.secho(
                        dedent("""
                                     Unable to locate workflow code. If you
                                     are a registering a Snakemake project,
                                     make sure to pass the Snakefile path with
                                     the --snakefile flag."""),
                        fg="red",
                    )
                    raise click.exceptions.Exit(1)
            else:
                assert snakefile is not None

                import latch.types.metadata as metadata

                from ..snakemake.serialize import (
                    get_snakemake_metadata_example,
                    snakemake_workflow_extractor,
                )

                new_meta = pkg_root / "latch_metadata" / "__init__.py"
                old_meta = pkg_root / "latch_metadata.py"
                if new_meta.exists():
                    click.echo(
                        f"Using metadata file {click.style(new_meta, italic=True)}"
                    )
                    import_module_by_path(new_meta)
                elif old_meta.exists():
                    click.echo(
                        f"Using metadata file {click.style(old_meta, italic=True)}"
                    )
                    import_module_by_path(old_meta)
                else:
                    click.echo("Trying to extract metadata from the Snakefile")
                    try:
                        snakemake_workflow_extractor(pkg_root, snakefile)
                    except (ImportError, FileNotFoundError):
                        traceback.print_exc()
                        click.secho(
                            "\n\n\n"
                            + "The above error occured when reading "
                            + "the Snakefile to extract workflow metadata.",
                            bold=True,
                            fg="red",
                        )
                        click.secho(
                            "\nIt is possible to avoid including the Snakefile"
                            " prior to registration by providing a"
                            " `latch_metadata.py` file in the workflow root.\nThis"
                            " way it is not necessary to install dependencies or"
                            " ensure that Snakemake inputs locally.",
                            fg="red",
                        )
                        click.secho("\nExample ", fg="red", nl=False)

                        snakemake_metadata_example = get_snakemake_metadata_example(
                            pkg_root.name
                        )
                        click.secho(f"`{new_meta}`", bold=True, fg="red", nl=False)
                        click.secho(
                            f" file:\n```\n{snakemake_metadata_example}```",
                            fg="red",
                        )
                        if click.confirm(
                            click.style(
                                "Generate example metadata file now?",
                                bold=True,
                                fg="red",
                            ),
                            default=True,
                        ):
                            new_meta.write_text(snakemake_metadata_example)

                            import platform

                            system = platform.system()
                            if system in {
                                "Windows",
                                "Linux",
                                "Darwin",
                            } and click.confirm(
                                click.style(
                                    "Open the generated file?", bold=True, fg="red"
                                ),
                                default=True,
                            ):
                                import subprocess

                                if system == "Linux":
                                    res = subprocess.run(
                                        ["xdg-open", new_meta]
                                    ).returncode
                                elif system == "Darwin":
                                    res = subprocess.run(["open", new_meta]).returncode
                                elif system == "Windows":
                                    import os

                                    res = os.system(str(new_meta.resolve()))
                                else:
                                    res = None

                                if res is not None and res != 0:
                                    click.secho("Failed to open file", fg="red")
                        sys.exit(1)

                if metadata._snakemake_metadata is None:
                    click.secho(
                        dedent(
                            """
                        Make sure a `latch_metadata.py` file exists in the Snakemake
                         project root.""",
                        ),
                        fg="red",
                    )
                    raise click.exceptions.Exit(1)

                # todo(kenny): support per container task and custom workflow
                # name for snakemake
                self.workflow_name = metadata._snakemake_metadata.name

            version_file = self.pkg_root / "version"
            try:
                self.version = version_file.read_text()
            except FileNotFoundError:
                self.version = "0.1.0"
                version_file.write_text(f"{self.version}\n")
                click.echo(
                    f"Created a version file with initial version {self.version}."
                )
            self.version = self.version.strip()

            if not self.disable_auto_version:
                hash = hash_directory(self.pkg_root)
                self.version = f"{self.version}-{hash[:6]}"
                click.echo(f"  {self.version}\n")

            if self.nucleus_check_version(self.version, self.workflow_name):
                click.secho(
                    f"\nVersion ({self.version}) already exists."
                    " Make sure that you've saved any changes you made.",
                    fg="red",
                    bold=True,
                )
                sys.exit(1)

            self.default_container = _Container(
                dockerfile=get_default_dockerfile(
                    self.pkg_root, wf_type=self.workflow_type
                ),
                image_name=self.image_tagged,
                pkg_dir=self.pkg_root,
            )

            if remote:
                # todo(maximsmol): connect only AFTER confirming registration
                self.ssh_key_path = self.pkg_root / ".latch/ssh_key"
                self.jump_key_path = self.pkg_root / ".latch/jump_key"
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

                def _patched_connect(self): ...

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

        wf_name = identifier_suffix_from_str(self.workflow_name).lower()
        wf_name = docker_image_name_illegal_pat.sub("_", wf_name)

        return f"{account_id}_{wf_name}"

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
        click.echo("Provisioning register instance. This may take a few minutes.")

        assert self.ssh_key_path is not None
        assert self.jump_key_path is not None

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
