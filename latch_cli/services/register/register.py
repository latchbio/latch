"""Service to register workflows."""

import base64
import contextlib
import os
import tempfile
from pathlib import Path
from typing import List, Union

import boto3
import requests

from latch_cli.services.register import RegisterCtx, RegisterOutput


def _print_build_logs(build_logs, image):
    print(f"\tBuilding Docker image for {image}")
    for x in build_logs:
        line = x.get("stream")
        error = x.get("error")
        if error is not None:
            print(f"\t\t{x}")
            raise OSError(f"Error when building image ~ {x}")
        elif line is not None:
            print(f"\t\t{line}", end="")


def _print_serialize_logs(serialize_logs, image):
    print(f"\tSerializing workflow in {image}:")
    for x in serialize_logs:
        print(f"\t\t{x}", end="")


def _print_upload_logs(upload_image_logs, image):
    print(f"\tUploading Docker image for {image}")
    prog_map = {}

    def _pp_prog_map(m):
        prog_chunk = ""
        i = 0
        for id, prog in m.items():
            if prog is None:
                continue
            prog_chunk += f"\t\t{id} ~ {prog}\n"
            i += 1
        if prog_chunk == "":
            return
        print(prog_chunk, end=f"\x1B[{i}A")

    for x in upload_image_logs:
        if (
            x.get("error") is not None
            and "denied: Your authorization token has expired." in x["error"]
        ):
            raise OSError(f"Docker authorization for {image} is expired.")
        prog_map[x.get("id")] = x.get("progress")
        _pp_prog_map(prog_map)


def _print_reg_resp(resp, image):
    print(f"\tRegistering {image} with LatchBio.")
    print("\tstdout:")
    for x in resp["stdout"].split("\n"):
        print(f"\t\t{x}")
    print("\tstderr:")
    for x in resp["stderr"].split("\n"):
        print(f"\t\t{x}")


def register(
    pkg_root: str,
    remote: Union[str, None] = None,
) -> RegisterOutput:
    """Registers a workflow, defined as python code, with Latch.

    Kicks off a three-legged OAuth2.0 flow outlined in `RFC6749`_.  Logic
    scaffolding this flow and detailed documentation can be found in the
    `latch.auth` package

    From a high-level, the user will be redirected to a browser and prompted to
    login. The SDK meanwhile spins up a callback server on a separate thread
    that will be hit when the browser login is successful with an access token.

    .. _RFC6749:
        https://datatracker.ietf.org/doc/html/rfc6749

    The major constituent steps are:

        - Constructing a Docker image
        - Serializing flyte objects within an instantiated container
        - Uploading the container with a latch-owned registry
        - Registering serialized objects + the container with latch.

    The Docker image is constructed by inferring relevant files + dependencies
    from the workflow package code itself. If a Dockerfile is provided
    explicitly, it will be used for image construction instead.

    The registration flow makes heavy use of `Flyte`_, and while the Latch SDK
    modifies many components to play nicely with Latch, eg. platform API,
    user-specific auth, the underlying concepts are nicely summarized in the
    `flytekit documentation`_.

    Args:
        pkg_root: A valid path pointing to the worklow code a user wishes to
            register. The path can be absolute or relative. The path is always
            a directory, with its structure exactly as constructed and
            described in the `cli.services.init` function.
        dockerfile: An optional valid path pointing to `Dockerfile`_ to define
            a custom container. If passed, the resulting container will be used
            as the environment to execute the registered workflow, allowing
            arbitrary binaries and libraries to be called from workflow code.
            However, be warned, this Dockerfile will be used *as is* - files
            must be copied correctly and shell variables must be set to ensure
            correct execution. See examples (TODO) for guidance.

    Example: ::

        register("./foo")
        register("/root/home/foo")

        register("/root/home/foo", dockerfile="./Dockerfile")
        register("/root/home/foo", requirements="./requirements.txt")

    .. _Flyte:
        https://docs.flyte.org
    .. _Dockerfile:
        https://docs.docker.com/engine/reference/builder/
    .. _flytekit documentation:
        https://docs.flyte.org/en/latest/concepts/registration.html
    """

    ctx = RegisterCtx(pkg_root)
    ctx.remote = remote
    print(f"Initializing registration for {pkg_root}")

    dockerfile = ctx.pkg_root.joinpath("Dockerfile")
    build_logs = build_image(ctx, dockerfile)
    _print_build_logs(build_logs, ctx.image_tagged)

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td).resolve()

        serialize_logs = _serialize_pkg(ctx, td_path)
        _print_serialize_logs(serialize_logs, ctx.image_tagged)

        upload_image_logs = _upload_pkg_image(ctx)
        _print_upload_logs(upload_image_logs, ctx.image_tagged)

        reg_resp = _register_serialized_pkg(ctx, td_path)
        _print_reg_resp(reg_resp, ctx.image_tagged)

    return RegisterOutput(
        build_logs=build_logs,
        serialize_logs=serialize_logs,
        registration_response=reg_resp,
    )


def _login(ctx: RegisterCtx):

    headers = {"Authorization": f"Bearer {ctx.token}"}
    data = {"pkg_name": ctx.image}
    response = requests.post(ctx.latch_image_api_url, headers=headers, json=data)
    try:
        response = response.json()
        access_key = response["tmp_access_key"]
        secret_key = response["tmp_secret_key"]
        session_token = response["tmp_session_token"]
    except KeyError as err:
        raise ValueError(f"malformed response on image upload: {response}") from err

    # TODO: cache
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


def build_image(
    ctx: RegisterCtx,
    dockerfile: Path,
) -> List[str]:

    _login(ctx)
    build_logs = ctx.dkr_client.build(
        path=str(dockerfile.parent),
        buildargs={"tag": ctx.full_image_tagged},
        tag=ctx.full_image_tagged,
        decode=True,
    )
    return build_logs


def _serialize_pkg(ctx: RegisterCtx, serialize_dir: Path) -> List[str]:

    _serialize_cmd = ["make", "serialize"]
    container = ctx.dkr_client.create_container(
        ctx.full_image_tagged,
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

    return [x.decode("utf-8") for x in logs]


def _upload_pkg_image(ctx: RegisterCtx) -> List[str]:

    return ctx.dkr_client.push(
        repository=ctx.full_image_tagged,
        stream=True,
        decode=True,
    )


def _register_serialized_pkg(ctx: RegisterCtx, serialize_dir: Path) -> dict:
    headers = {"Authorization": f"Bearer {ctx.token}"}

    with contextlib.ExitStack() as stack:
        serialize_files = {"version": ctx.version.encode("utf-8")}
        for dirname, dirnames, fnames in os.walk(serialize_dir):
            for filename in fnames + dirnames:
                file = Path(dirname).resolve().joinpath(filename)
                serialize_files[file.name] = stack.enter_context(open(file, "rb"))

        response = requests.post(
            ctx.latch_register_api_url,
            headers=headers,
            files=serialize_files,
        )

    return response.json()
