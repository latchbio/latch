import base64
import os
from dataclasses import asdict, dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import boto3.session
import click
import docker
import docker.auth
import docker.errors
import paramiko
from docker.transport import SSHHTTPAdapter

from latch.utils import current_workspace
from latch_cli import tinyrequests
from latch_sdk_config.latch import NUCLEUS_URL, config

from ...utils import TemporarySSHCredentials, get_auth_header
from ..register.register import print_and_write_build_logs, print_upload_logs

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..register.utils import DockerBuildLogItem


@dataclass
class DockerCredentials:
    username: str
    password: str


def get_credentials(image: str) -> DockerCredentials:
    response = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/sdk/initiate-image-upload"),
        headers={"Authorization": get_auth_header()},
        json={"pkg_name": image, "ws_account_id": current_workspace()},
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

    return DockerCredentials(username=username, password=password)


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


def remote_dbnp(
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
