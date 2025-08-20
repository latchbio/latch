import base64
import os
from dataclasses import asdict, dataclass
from json import JSONDecodeError
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Iterable, Optional
from urllib.parse import urljoin

import boto3.session
import click
import docker
import docker.auth
import docker.errors
import gql
import paramiko
from docker.transport import SSHHTTPAdapter

from latch.utils import current_workspace
from latch_cli import tinyrequests
from latch_cli.docker_utils import get_default_dockerfile
from latch_sdk_config.latch import NUCLEUS_URL, config
from latch_sdk_gql.execute import execute

from ...centromere.ast_parsing import get_flyte_objects
from ...constants import docker_image_name_illegal_pat, latch_constants
from ...utils import (
    TemporarySSHCredentials,
    WorkflowType,
    get_auth_header,
    hash_directory,
    identifier_suffix_from_str,
)
from .register import print_and_write_build_logs, print_upload_logs

if TYPE_CHECKING:
    from .utils import DockerBuildLogItem


@dataclass
class DockerCredentials:
    username: str
    password: str


def get_credentials(image: str) -> DockerCredentials:
    response = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/sdk/initiate-image-upload"),
        headers={"Authorization": get_auth_header()},
        json={
            "pkg_name": f"{config.dkr_repo}/{image}",
            "ws_account_id": current_workspace(),
        },
    )

    try:
        data = response.json()

        # todo(ayush): compute the authorization token in the endpoint and send it directly
        access_key = data["tmp_access_key"]
        secret_key = data["tmp_secret_key"]
        session_token = data["tmp_session_token"]
    except (JSONDecodeError, KeyError) as err:
        raise ValueError(
            f"malformed response on image upload: {response.content}"
        ) from err

    ecr = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name="us-west-2",
    ).client("ecr")

    token = ecr.get_authorization_token()["authorizationData"][0]["authorizationToken"]
    username, password = base64.b64decode(token).decode("utf-8").split(":")

    return DockerCredentials(username, password)


def get_local_docker_client() -> docker.APIClient:
    try:
        host = os.environ.get("DOCKER_HOST")

        if host is None or host == "":
            return docker.APIClient(base_url="unix://var/run/docker.sock")

        cert_path = os.environ.get("DOCKER_CERT_PATH")
        if cert_path == "":
            cert_path = None

        tls_verify = os.environ.get("DOCKER_TLS_VERIFY") != ""
        enable_tls = tls_verify or cert_path is not None

        if not enable_tls:
            return docker.APIClient(host)

        if cert_path is None:
            cert_path = Path.home() / ".docker"
        else:
            cert_path = Path(cert_path)

        return docker.APIClient(
            host,
            tls=docker.tls.TLSConfig(
                client_cert=(str(cert_path / "cert.pem"), str(cert_path / "key.pem")),
                ca_cert=str(cert_path / "ca.pem"),
                verify=tls_verify,
            ),
        )
    except docker.errors.DockerException as de:
        click.secho(
            "Docker is not running. Make sure that Docker is running before attempting to register a workflow.",
            fg="red",
        )
        raise click.exceptions.Exit(1) from de


def dbnp(
    client: docker.APIClient,
    pkg_root: Path,
    image: str,
    version: str,
    dockerfile: Path,
    *,
    progress_plain: bool,
):
    credentials = get_credentials(image)
    client._auth_configs = docker.auth.AuthConfig({  # noqa: SLF001
        "auths": {config.dkr_repo: asdict(credentials)}
    })

    build_logs: Iterable[DockerBuildLogItem] = client.build(
        path=str(pkg_root),
        tag=f"{config.dkr_repo}/{image}:{version}",
        dockerfile=str(dockerfile),
        buildargs={"tag": f"{config.dkr_repo}/{image}:{version}"},
        decode=True,
    )

    print_and_write_build_logs(
        build_logs, image, pkg_root, progress_plain=progress_plain
    )

    upload_logs = client.push(
        repository=f"{config.dkr_repo}/{image}",
        tag=version,
        stream=True,
        decode=True,
        auth_config=asdict(credentials),
    )

    print_upload_logs(upload_logs, image)


def remote_register(
    pkg_root: Path, image: str, version: str, dockerfile: Path, *, progress_plain: bool
):
    key_path = pkg_root / ".latch" / "ssh_key"

    with TemporarySSHCredentials(key_path) as keys:
        response = tinyrequests.post(
            urljoin(NUCLEUS_URL, "/sdk/provision-centromere"),
            headers={"Authorization": get_auth_header()},
            json={"public_key": keys.public_key},
        )

        resp = response.json()
        try:
            hostname = resp["ip"]
            username = resp["username"]
        except KeyError as e:
            raise ValueError(
                f"Malformed response from request to provision centromere {resp}"
            ) from e

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy)

        pkey = paramiko.PKey.from_path(key_path)
        ssh.connect(hostname, username=username, pkey=pkey)

        transport = ssh.get_transport()
        assert transport is not None

        transport.set_keepalive(30)

        def _patched_connect(self: SSHHTTPAdapter): ...

        def _patched_create_paramiko_client(self: SSHHTTPAdapter, base_url: str):
            self.ssh_client = ssh

        SSHHTTPAdapter._create_paramiko_client = _patched_create_paramiko_client
        SSHHTTPAdapter._connect = _patched_connect

        # todo(ayush): drop pydocker and connect to the socket directly
        client = docker.APIClient("ssh://fake", version="1.41")

        dbnp(
            client, pkg_root, image, version, dockerfile, progress_plain=progress_plain
        )


def register_staging(
    pkg_root: Path,
    *,
    disable_auto_version: bool = False,
    disable_git_version: bool = False,
    remote: bool = False,
    skip_confirmation: bool = False,
    wf_module: Optional[str] = None,
    progress_plain: bool = False,
    dockerfile_path: Optional[Path] = None,
):
    wf_module = wf_module if wf_module is not None else "wf"
    module_path = pkg_root / Path(wf_module.replace(".", "/"))

    if dockerfile_path is None:
        dockerfile_path = get_default_dockerfile(
            pkg_root, wf_type=WorkflowType.latchbiosdk
        )

    try:
        flyte_objects = get_flyte_objects(module_path)
    except ModuleNotFoundError as e:
        click.secho(
            dedent(
                f"""
                Unable to locate workflow module `{wf_module}` in `{pkg_root.resolve()}`. Check that:

                1. {module_path} exists.
                2. Package `{wf_module}` is an absolute importable Python path (e.g. `workflows.my_workflow`).
                3. All directories in `{module_path}` contain an `__init__.py` file."""
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    wf_name: Optional[str] = None

    name_path = pkg_root / latch_constants.pkg_workflow_name
    if name_path.exists():
        click.echo(f"Parsing workflow name from {name_path}.")
        wf_name = name_path.read_text().strip()

    if wf_name is None:
        click.echo(f"Searching {module_path} for @workflow function.")
        for obj in flyte_objects:
            if obj.type != "workflow":
                continue

            wf_name = obj.name
            break

    if wf_name is None:
        click.secho(
            dedent(f"""
            Unable to find a function decorated with `@workflow` in {module_path}. Please double check that
            the value of `--workflow-module` is correct.
            """),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    version_file = pkg_root / "version"
    try:
        version_base = version_file.read_text().strip()
    except OSError:
        if not skip_confirmation and not click.confirm(
            "Could not find a `version` file in the package root. One will be created. Proceed?"
        ):
            return

        version_base = "0.1.0"
        version_file.write_text(version_base)
        click.echo(f"Created a version file with initial version {version_base}.")

    components: list[str] = [version_base, "staging"]

    if disable_auto_version:
        click.echo("Skipping version tagging due to `--disable-auto-version`")
    elif disable_git_version:
        click.echo("Skipping git version tagging due to `--disable-git-version`")

    if not disable_auto_version and not disable_git_version:
        try:
            from git import GitError, Repo

            try:
                repo = Repo(pkg_root)
                sha = repo.head.commit.hexsha[:6]
                components.append(sha)
                click.echo(f"Tagging version with git commit {sha}.")
                click.secho(
                    "  Disable with --disable-git-version/-G", dim=True, italic=True
                )

                if repo.is_dirty():
                    components.append("wip")
                    click.secho(
                        "  Repo contains uncommitted changes - tagging version with `wip`",
                        italic=True,
                    )
            except GitError:
                pass
        except ImportError:
            pass

    if not disable_auto_version:
        sha = hash_directory(pkg_root, silent=True)[:6]
        components.append(sha)
        click.echo(f"Tagging version with directory checksum {sha}.")
        click.secho("  Disable with --disable-auto-version/-d", dim=True, italic=True)

    version = "-".join(components)

    click.echo()

    res = execute(
        gql.gql("""
        query LatestVersion($wsId: BigInt!, $name: String!, $version: String!) {
            latchDevelopStagingImages(
                filter: {
                    ownerId: { equalTo: $wsId }
                    workflowName: { equalTo: $name }
                    version: { equalTo: $version }
                }
            ) {
                totalCount
            }
        }
        """),
        {"wsId": current_workspace(), "name": wf_name, "version": version},
    )

    if res["latchDevelopStagingImages"]["totalCount"] != 0:
        click.secho(
            f"Version `{version}` already exists for workflow `{wf_name}` in workspace `{current_workspace()}`. ",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if not skip_confirmation:
        if not click.confirm("Start registration?"):
            click.secho("Cancelled", bold=True)
            return
    else:
        click.secho("Skipping confirmation because of --yes", bold=True)

    image_suffix = docker_image_name_illegal_pat.sub(
        "_", identifier_suffix_from_str(wf_name).lower()
    )
    image_prefix = current_workspace()
    if len(image_prefix) == 1:
        # note(ayush): the sins of our past continue to haunt us
        image_prefix = f"x{image_prefix}"

    image = f"{image_prefix}_{image_suffix}"

    if remote:
        remote_register(
            pkg_root, image, version, dockerfile_path, progress_plain=progress_plain
        )
    else:
        client = get_local_docker_client()

        dbnp(
            client,
            pkg_root,
            image,
            version,
            dockerfile_path,
            progress_plain=progress_plain,
        )

    execute(
        gql.gql("""
        mutation AddStagingImage(
            $wsId: BigInt!
            $workflowName: String!
            $version: String!
        ) {
            createLatchDevelopStagingImage(
                input: {
                    latchDevelopStagingImage: {
                        ownerId: $wsId
                        workflowName: $workflowName
                        version: $version
                    }
                }
            ) {
                clientMutationId
            }
        }
        """),
        {"wsId": current_workspace(), "workflowName": wf_name, "version": version},
    )

    click.secho("Successfully staged workflow.", fg="green")
