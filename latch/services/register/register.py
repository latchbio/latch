"""
services.register
~~~~~
Registers workflows with the latch platform.
"""

import base64
import json
import os
import tarfile
import tempfile
import textwrap
from io import BytesIO
from pathlib import Path
from typing import List, Union

import boto3
import requests
from latch.services.register import RegisterCtx, RegisterOutput


def register(
    pkg_root: str,
    dockerfile: Union[str, None] = None,
    pkg_name: Union[str, None] = None,
) -> RegisterOutput:
    """This service will register a workflow defined as python code with latch.

    The major constituent steps are:

        - Constructing a Docker image
        - Serializing flyte objects within an instantiated container
        - Uploading the container with a latch-owned registry
        - Registering serialized objects + the container with latch.

    The Docker image is constructed by inferring relevant files + dependencies
    from the workflow package code itself. If a Dockerfile is provided
    explicitly, it will be used for image construction instead.
    """

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

    ctx = RegisterCtx(pkg_root)
    print(f"Initializing registration for {pkg_root}")

    if dockerfile is not None:
        dockerfile = Path(dockerfile).resolve()
        if not dockerfile.exists():
            raise OSError(f"Provided Dockerfile {dockerfile} does not exist.")

    build_logs = _build_image(ctx, dockerfile, pkg_name)
    _print_build_logs(build_logs, ctx.image_tagged)

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td).resolve()

        serialize_logs = _serialize_pkg(ctx, td_path)
        _print_serialize_logs(serialize_logs, ctx.image_tagged)

        upload_image_logs = _upload_pkg_image(ctx)
        _print_upload_logs(upload_image_logs, ctx.image_tagged)

        reg_resp = _register_serialized_pkg(ctx, td_path)
        _print_reg_resp(reg_resp, ctx.image_tagged)

    return (
        RegisterOutput(
            build_logs=build_logs,
            serialize_logs=serialize_logs,
            registration_response=reg_resp,
        ),
        ctx.pkg_name,
    )


def _build_image(
    ctx: RegisterCtx, dockerfile: Union[None, Path], pkg_name: Union[None, str]
) -> List[str]:

    if dockerfile is not None:
        if pkg_name is not None:
            ctx.pkg_name = pkg_name
            try:
                version_file = ctx.pkg_root.joinpath(pkg_name).joinpath("version")
                with open(version_file, "r") as vf:
                    ctx.version = vf.read().strip()
            except:
                raise ValueError(
                    f"Unable to extract pkg version ~ root:{str(ctx.pkg_root)}"
                    f" pkg_name:{pkg_name}"
                )
        else:
            raise ValueError(
                "You passed a custom Dockerfile but failed to pass"
                " a value to --pkg_name. This option is mandatory if an explicity"
                " Dockerfile is provided."
            )
        build_logs = ctx.dkr_client.build(
            path=str(dockerfile.parent),
            buildargs={"tag": ctx.full_image_tagged},
            tag=ctx.full_image_tagged,
            decode=True,
        )
        return build_logs

    # Contruct tarball holding docker build context
    # We want to construct a custom context that only has package files + our
    # dockerfile object injected directly from memory.
    def _build_file_list(root: str):
        files = []
        for dirname, dirnames, fnames in os.walk(root):
            for filename in fnames + dirnames:
                longpath = os.path.join(dirname, filename)
                files.append(longpath.replace(root, "", 1).lstrip("/"))
        return files

    with tempfile.NamedTemporaryFile() as f:
        with tarfile.open(mode="w", fileobj=f) as t:

            for path in _build_file_list(str(ctx.pkg_root)):
                full_path = Path(ctx.pkg_root).resolve().joinpath(path)
                i = t.gettarinfo(full_path, arcname=path)

                if i.isfile():
                    try:
                        if full_path.name == "__init__.py":
                            pkg_name_candidate = full_path.parent.name
                            if ctx.pkg_name is not None:
                                raise ValueError(
                                    "Can only register one"
                                    " workflow package at a time."
                                    " Attempted to register both"
                                    f" {ctx.pkg_name} and"
                                    f" {pkg_name_candidate}"
                                )
                            else:
                                ctx.pkg_name = pkg_name_candidate

                        if full_path.name == "version":
                            with open(full_path, "r") as v:
                                ctx.version = v.read().strip()
                                v.seek(0)
                        with open(full_path, "rb") as fp:
                            t.addfile(i, fp)

                    except OSError:
                        raise OSError(f"Can not read file in context: {full_path}")
                else:
                    # Directories, FIFOs, symlinks don't need to be read.
                    t.addfile(i, None)

            if ctx.version is None:
                raise ValueError(
                    "No version file found in {root}. This file is required"
                    " and should contain the package version in plain text."
                )

            dockerfile = textwrap.dedent(
                f"""
                    FROM {ctx.dkr_repo}/wf-base:wf-base-d2fb-main


                    COPY {ctx.pkg_name} /root/{ctx.pkg_name}
                    WORKDIR /root

                    ARG tag
                    ENV FLYTE_INTERNAL_IMAGE $tag
                    """
            )
            dockerfile = BytesIO(dockerfile.encode("utf-8"))
            dfinfo = tarfile.TarInfo("Dockerfile")
            dfinfo.size = len(dockerfile.getvalue())
            dockerfile.seek(0)
            t.addfile(dfinfo, dockerfile)
            f.seek(0)

            build_logs = ctx.dkr_client.build(
                fileobj=f,
                custom_context=True,
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
        ).client("ecr")
        token = client.get_authorization_token()["authorizationData"][0][
            "authorizationToken"
        ]
    except client.exceptions.ClientError as err:
        raise ValueError(
            f"unable to retreive an ecr login token for user {account_id}"
        ) from err

    user, password = base64.b64decode(token).decode("utf-8").split(":")
    ctx.dkr_client.login(
        username=user,
        password=password,
        registry=ctx.dkr_repo,
    )
    return ctx.dkr_client.push(
        repository=ctx.full_image_tagged,
        stream=True,
        decode=True,
    )


def _register_serialized_pkg(ctx: RegisterCtx, serialize_dir: Path) -> dict:

    files = {"version": ctx.version.encode("utf-8")}
    for dirname, dirnames, fnames in os.walk(serialize_dir):
        for filename in fnames + dirnames:
            file = Path(dirname).resolve().joinpath(filename)
            files[file.name] = open(file, "rb")

    headers = {"Authorization": f"Bearer {ctx.token}"}
    response = requests.post(ctx.latch_register_api_url, headers=headers, files=files)
    return response.json()
