import base64
import contextlib
import importlib.machinery as im
import importlib.util as iu
import io
import os
import sys
import typing
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

import boto3
import click
import docker
import requests
from latch_sdk_config.latch import config

from latch.utils import current_workspace

if TYPE_CHECKING:
    from ...centromere.ctx import _CentromereCtx
else:
    _CentromereCtx = ""


# todo(maximsmol): only login if the credentials are expired
def _docker_login(ctx: _CentromereCtx):
    assert ctx.dkr_client is not None

    headers = {"Authorization": f"Bearer {ctx.token}"}
    data = {"pkg_name": ctx.image, "ws_account_id": current_workspace()}
    response = requests.post(ctx.latch_image_api_url, headers=headers, json=data)

    try:
        response = response.json()
        access_key = response["tmp_access_key"]
        secret_key = response["tmp_secret_key"]
        session_token = response["tmp_session_token"]
    except KeyError as err:
        raise ValueError(f"malformed response on image upload: {response}") from err

    try:
        client = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name="us-west-2",
        ).client("ecr")
        token = client.get_authorization_token()["authorizationData"][0][
            "authorizationToken"
        ]
    except Exception as err:
        raise ValueError(
            f"unable to retreive an ecr login token for user {ctx.account_id}"
        ) from err

    auth = ctx.dkr_client._auth_configs
    store_name = auth.get_credential_store(ctx.dkr_repo)
    if store_name is not None:
        store = auth._get_store_instance(store_name)
        try:
            store.erase(ctx.dkr_repo)
        # To handle: "Credentials store docker-credential-osxkeychain exited
        # with "The specified item could not be found in the keychain.""
        except docker.credentials.errors.StoreError:
            pass

    user, password = base64.b64decode(token).decode("utf-8").split(":")
    res = ctx.dkr_client.login(
        username=user, password=password, registry=ctx.dkr_repo, reauth=True
    )
    assert res["Status"] == "Login Succeeded"


class DockerBuildLogItem(TypedDict):
    message: Optional[str]
    error: Optional[str]
    stream: Optional[str]


def build_image(
    ctx: _CentromereCtx,
    image_name: str,
    context_path: Path,
    dockerfile: Optional[Path] = None,
) -> Iterable[DockerBuildLogItem]:
    assert ctx.dkr_client is not None

    _docker_login(ctx)
    build_logs = ctx.dkr_client.build(
        path=str(context_path),
        dockerfile=str(dockerfile) if dockerfile is not None else None,
        buildargs={"tag": f"{ctx.dkr_repo}/{image_name}"},
        tag=f"{ctx.dkr_repo}/{image_name}",
        decode=True,
    )

    return build_logs


def upload_image(ctx: _CentromereCtx, image_name: str) -> List[str]:
    assert ctx.dkr_client is not None
    return ctx.dkr_client.push(
        repository=f"{ctx.dkr_repo}/{image_name}",
        stream=True,
        decode=True,
    )


def serialize_pkg_in_container(
    ctx: _CentromereCtx,
    image_name: str,
    serialize_dir: str,
    wf_name_override: Optional[str] = None,
) -> Tuple[List[str], str]:
    assert ctx.dkr_client is not None

    _env = {"LATCH_DKR_REPO": ctx.dkr_repo, "LATCH_VERSION": ctx.version}
    if wf_name_override is not None:
        _env["LATCH_WF_NAME_OVERRIDE"] = wf_name_override

    if ctx.git_commit_hash is not None:
        click.secho(
            f"Tagging workflow version with git commit {ctx.git_commit_hash}", fg="blue"
        )
        _env["GIT_COMMIT_HASH"] = ctx.git_commit_hash
        _env["GIT_IS_DIRTY"] = str(ctx.git_is_dirty)

    _serialize_cmd = ["make", "serialize"]
    container = ctx.dkr_client.create_container(
        f"{ctx.dkr_repo}/{image_name}",
        command=_serialize_cmd,
        volumes=[serialize_dir],
        environment=_env,
        host_config=ctx.dkr_client.create_host_config(
            binds={
                serialize_dir: {
                    "bind": "/tmp/output",
                    "mode": "rw",
                },
            }
        ),
    )
    container_id = typing.cast(str, container.get("Id"))
    ctx.dkr_client.start(container_id)
    logs = typing.cast(Iterable[bytes], ctx.dkr_client.logs(container_id, stream=True))

    return [x.decode("utf-8") for x in logs], container_id


def register_serialized_pkg(
    files: List[Path],
    token: Optional[str],
    version: str,
    workspace_id: str,
    latch_register_url: str = config.api.workflow.register,
) -> object:
    if token is None:
        token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID", "")
        if token != "":
            headers = {"Authorization": f"Latch-Execution-Token {token}"}
        else:
            raise OSError(
                "The environment variable FLYTE_INTERNAL_EXECUTION_ID does not exist"
            )
    else:
        headers = {"Authorization": f"Bearer {token}"}

    serialize_files: Dict[str, Union[bytes, io.BufferedReader]] = {
        "version": version.encode("utf-8"),
        ".latch_ws": workspace_id.encode("utf-8"),
    }
    with contextlib.ExitStack() as stack:
        for file in files:
            fh = open(file, "rb")
            stack.enter_context(fh)

            serialize_files[fh.name] = fh

        response = requests.post(
            latch_register_url,
            headers=headers,
            files=serialize_files,
        )
        response.raise_for_status()
        return response.json()


def import_module_by_path(x: Path, *, module_name: str = "latch_metadata"):
    spec = iu.spec_from_file_location(module_name, x)
    assert spec is not None
    assert spec.loader is not None

    module = iu.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module
