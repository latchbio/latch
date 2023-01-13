"Utilites for registration."

import base64
import contextlib
from pathlib import Path
from typing import List, Optional

import boto3
import requests

from latch_cli.centromere.ctx import _CentromereCtx
from latch_cli.utils import current_workspace


def _docker_login(ctx: _CentromereCtx):
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

    user, password = base64.b64decode(token).decode("utf-8").split(":")
    ctx.dkr_client.login(
        username=user,
        password=password,
        registry=ctx.dkr_repo,
    )


def _build_image(
    ctx: _CentromereCtx,
    image_name: str,
    context_path: Path,
    dockerfile: Optional[Path] = None,
) -> List[str]:
    _docker_login(ctx)
    if dockerfile is not None:
        dockerfile = str(dockerfile)
    build_logs = ctx.dkr_client.build(
        path=str(context_path),
        dockerfile=dockerfile,
        buildargs={"tag": f"{ctx.dkr_repo}/{image_name}"},
        tag=f"{ctx.dkr_repo}/{image_name}",
        decode=True,
    )

    return build_logs


def _upload_image(ctx: _CentromereCtx, image_name: str) -> List[str]:
    return ctx.dkr_client.push(
        repository=f"{ctx.dkr_repo}/{image_name}",
        stream=True,
        decode=True,
    )


def _serialize_pkg_in_container(
    ctx: _CentromereCtx, image_name: str, serialize_dir: Path
) -> List[str]:
    _serialize_cmd = ["make", "serialize"]
    container = ctx.dkr_client.create_container(
        f"{ctx.dkr_repo}/{image_name}",
        command=_serialize_cmd,
        volumes=[str(serialize_dir)],
        host_config=ctx.dkr_client.create_host_config(
            binds={
                str(serialize_dir): {
                    "bind": "/tmp/output",
                    "mode": "rw",
                },
            }
        ),
    )
    container_id = container.get("Id")
    ctx.dkr_client.start(container_id)
    logs = ctx.dkr_client.logs(container_id, stream=True)

    return [x.decode("utf-8") for x in logs], container_id


def _register_serialized_pkg(ctx: _CentromereCtx, files: List[Path]) -> dict:
    headers = {"Authorization": f"Bearer {ctx.token}"}

    serialize_files = {
        "version": ctx.version.encode("utf-8"),
        ".latch_ws": current_workspace().encode("utf-8"),
    }
    with contextlib.ExitStack() as stack:
        file_handlers = [stack.enter_context(open(file, "rb")) for file in files]
        for fh in file_handlers:
            serialize_files[fh.name] = fh

        response = requests.post(
            ctx.latch_register_api_url,
            headers=headers,
            files=serialize_files,
        )

    return response.json()
