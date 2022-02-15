"""Service to register workflows."""

import base64
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
    dockerfile: Union[str, None] = None,
    requirements: Union[str, None] = None,
) -> RegisterOutput:
    """Registers a workflow, defined as python code, with Latch.

    Kicks off a three-legged OAuth2.0 flow outlined in `this RFC`_.  Logic
    scaffolding this flow and detailed documentation can be found in the
    `latch.auth` package

    From a high-level, the user will be redirected to a browser and prompted to
    login. The SDK meanwhile spins up a callback server on a separate thread
    that will be hit when the browser login is successful with an access token.

    .. _this RFC
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
            described in the `latch.services.init` function.
        dockerfile: An optional valid path pointing to `Dockerfile`_ to define
            a custom container. If passed, the resulting container will be used
            as the environment to execute the registered workflow, allowing
            arbitrary binaries and libraries to be called from workflow code.
            However, be warned, this Dockerfile will be used *as is* - files
            must be copied correctly and shell variables must be set to ensure
            correct execution. See examples (TODO) for guidance.
        requirements: An optional valid path pointing to `requirements.txt`
            file containing a list of python libraries in the format produced
            by `pip freeze` to install within the container that the workflow
            will execute.

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
    print(f"Initializing registration for {pkg_root}")

    if dockerfile is not None:
        dockerfile = Path(dockerfile).resolve()
        if not dockerfile.exists():
            raise OSError(f"Provided Dockerfile {dockerfile} does not exist.")

    if requirements is not None:
        if dockerfile is not None:
            raise ValueError(
                "Cannot provide both a dockerfile -"
                f" {str(dockerfile)} and requirements file {requirements}"
            )
        requirements = Path(requirements).resolve()
        if not requirements.exists():
            raise OSError(f"Provided requirements file {requirements} does not exist.")

    build_logs = _build_image(ctx, dockerfile, requirements)
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

    # except client.exceptions.ClientError as err:
    #    raise ValueError(
    #        f"unable to retreive an ecr login token for user {ctx.account_id}"
    #    ) from err

    user, password = base64.b64decode(token).decode("utf-8").split(":")
    ctx.dkr_client.login(
        username=user,
        password=password,
        registry=ctx.dkr_repo,
    )


def _build_image(
    ctx: RegisterCtx,
    dockerfile: Union[None, Path] = None,
    requirements: Union[None, Path] = None,
) -> List[str]:

    if dockerfile is not None:
        _login(ctx)
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

            # TODO: docker build context is from the perspective of one
            # directory up.
            for path in _build_file_list(str(ctx.pkg_root.parent)):
                full_path = Path(ctx.pkg_root.parent).resolve().joinpath(path)
                i = t.gettarinfo(full_path, arcname=path)
                if i.isfile():
                    try:
                        with open(full_path, "rb") as fp:
                            t.addfile(i, fp)
                    except OSError as e:
                        raise OSError(
                            f"Can not read file in context: {full_path}"
                        ) from e
                else:
                    # Directories, FIFOs, symlinks don't need to be read.
                    t.addfile(i, None)

            fk_config_file = textwrap.dedent(
                f"""
                    [sdk]
                    workflow_packages={ctx.pkg_root.name}
                    python_venv=flytekit_venv
                    """
            )
            fk_config_file = BytesIO(fk_config_file.encode("utf-8"))
            fcfinfo = tarfile.TarInfo("flytekit.config")
            fcfinfo.size = len(fk_config_file.getvalue())
            fk_config_file.seek(0)
            t.addfile(fcfinfo, fk_config_file)

            if requirements is not None:

                requirements_cmds = textwrap.dedent(
                    """
                            COPY requirements.txt /root
                            RUN python3 -m pip install -r requirements.txt
                        """
                )
                with open(requirements) as r:
                    requirements = BytesIO(r.read().encode("utf-8"))
                rinfo = tarfile.TarInfo("requirements.txt")
                rinfo.size = len(requirements.getvalue())
                requirements.seek(0)
                t.addfile(rinfo, requirements)
            else:
                requirements_cmds = ""

            dockerfile = textwrap.dedent(
                f"""
                    FROM {ctx.dkr_repo}/wf-base:wf-base-d2fb-main


                    COPY flytekit.config /root
                    COPY {ctx.pkg_root.name} /root/{ctx.pkg_root.name}
                    WORKDIR /root
                    RUN python3 -m pip install --upgrade latch

                    {requirements_cmds}

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

            _login(ctx)
            return ctx.dkr_client.build(
                fileobj=f,
                custom_context=True,
                buildargs={"tag": ctx.full_image_tagged},
                tag=ctx.full_image_tagged,
                decode=True,
            )


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

    files = {"version": ctx.version.encode("utf-8")}
    for dirname, dirnames, fnames in os.walk(serialize_dir):
        for filename in fnames + dirnames:
            file = Path(dirname).resolve().joinpath(filename)
            files[file.name] = open(file, "rb")

    headers = {"Authorization": f"Bearer {ctx.token}"}
    response = requests.post(ctx.latch_register_api_url, headers=headers, files=files)
    return response.json()
