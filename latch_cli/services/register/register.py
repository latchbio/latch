"""Service to register workflows."""

import contextlib
import functools
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import List, Optional

from scp import SCPClient

from latch_cli.centromere.ctx import _CentromereCtx
from latch_cli.centromere.utils import _TmpDir
from latch_cli.services.register.constants import ANSI_REGEX, MAX_LINES
from latch_cli.services.register.utils import (
    _build_image,
    _register_serialized_pkg,
    _serialize_pkg_in_container,
    _upload_image,
)

print = functools.partial(print, flush=True)


def _delete_lines(lines: List[str]):
    """Deletes the previous len(lines) lines, assuming cursor is on a
    new line just below the first line to be deleted"""
    for _ in lines:
        print("\x1b[1F\x1b[0G\x1b[2K", end="")
    return []


def _print_window(curr_lines: List[str], line: str):
    """Prints the lines curr_lines[1:] and line, overwriting curr_lines
    in the process"""
    if line == "":
        return curr_lines
    elif len(curr_lines) >= MAX_LINES:
        line = ANSI_REGEX.sub("", line)
        new_lines = curr_lines[len(curr_lines) - MAX_LINES + 1 :]
        new_lines.append(line)
        _delete_lines(curr_lines)
        for s in new_lines:
            print("\x1b[38;5;245m" + s + "\x1b[0m")
        return new_lines
    else:
        print("\x1b[38;5;245m" + line + "\x1b[0m")
        curr_lines.append(line)
        return curr_lines


def _print_and_save_build_logs(build_logs, image: str, pkg_root: Path):
    print(f"Building Docker image for {image}")

    logs_path = Path(pkg_root).joinpath(".logs").joinpath(image).resolve()
    logs_path.mkdir(parents=True, exist_ok=True)
    with open(logs_path.joinpath("docker-build-logs.txt"), "w") as save_file:
        r = re.compile("^Step [0-9]+/[0-9]+ :")
        curr_lines = []
        for x in build_logs:
            # for dockerfile parse errors
            message: str = x.get("message")
            if message is not None:
                save_file.write(f"{message}\n")
                raise ValueError(message)

            lines: str = x.get("stream")
            error: str = x.get("error")
            if error is not None:
                save_file.write(f"{error}\n")
                raise OSError(f"Error when building image ~ {error}")
            if lines:
                save_file.write(f"{lines}\n")
                for line in lines.split("\n"):
                    curr_terminal_width = shutil.get_terminal_size()[0]
                    if len(line) > curr_terminal_width:
                        line = line[: curr_terminal_width - 3] + "..."

                    if r.match(line):
                        curr_lines = _delete_lines(curr_lines)
                        print("\x1b[38;5;33m" + line + "\x1b[0m")
                    else:
                        curr_lines = _print_window(curr_lines, line)
        _delete_lines(curr_lines)


def _print_upload_logs(upload_image_logs, image):
    print(f"Uploading Docker image for {image}")
    prog_map = {}

    def _pp_prog_map(prog_map, prev_lines):
        if prev_lines > 0:
            print("\x1b[2K\x1b[1E" * prev_lines + f"\x1b[{prev_lines}F", end="")
        prog_chunk = ""
        i = 0
        for id, prog in prog_map.items():
            if prog is None:
                continue
            prog_chunk += f"{id} ~ {prog}\n"
            i += 1
        if prog_chunk == "":
            return 0
        print(prog_chunk, end=f"\x1B[{i}A")
        return i

    prev_lines = 0

    for x in upload_image_logs:
        if (
            x.get("error") is not None
            and "denied: Your authorization token has expired." in x["error"]
        ):
            raise OSError(f"Docker authorization for {image} is expired.")
        prog_map[x.get("id")] = x.get("progress")
        prev_lines = _pp_prog_map(prog_map, prev_lines)


def _print_reg_resp(resp, image):
    print(f"Registering {image} with LatchBio.")
    version = image.split(":")[1]

    if not resp.get("success"):
        error_str = f"Error registering {image}\n\n"
        if resp.get("stderr") is not None:
            for line in resp.get("stderr").split("\n"):
                if line and not line.startswith('{"json"'):
                    error_str += line + "\n"
        if "task with different structure already exists" in error_str:
            error_str = f"This version ({version}) already exists."
            " Make sure that you've saved any changes you made."
        raise ValueError(error_str)
    elif not "Successfully registered file" in resp["stdout"]:
        raise ValueError(
            f"This version ({version}) already exists."
            " Make sure that you've saved any changes you made."
        )
    else:
        print(resp.get("stdout"))


def _print_serialize_logs(serialize_logs, image):
    print(f"Serializing workflow in {image}:")
    for x in serialize_logs:
        print(x, end="")


def _build_and_serialize(
    ctx: _CentromereCtx,
    image_name: str,
    context_path: Path,
    tmp_dir: Path,
    dockerfile: Optional[Path] = None,
):
    """Encapsulates build, serialize, push flow needed for each dockerfile

    - Build image
    - Serialize workflow in image
    - Save proto files to passed temporary directory
    - Push image
    """

    build_logs = _build_image(ctx, image_name, context_path, dockerfile)
    _print_and_save_build_logs(build_logs, image_name, ctx.pkg_root)

    serialize_logs, container_id = _serialize_pkg_in_container(ctx, image_name, tmp_dir)
    _print_serialize_logs(serialize_logs, image_name)
    exit_status = ctx.dkr_client.wait(container_id)
    if exit_status["StatusCode"] != 0:
        raise ValueError(
            f"Serialization exited with nonzero exit code: {exit_status['Error']}"
        )

    upload_image_logs = _upload_image(ctx, image_name)
    _print_upload_logs(upload_image_logs, image_name)


def _recursive_list(directory: Path) -> List[Path]:
    files = []
    for dirname, dirnames, fnames in os.walk(directory):
        for filename in fnames + dirnames:
            files.append(Path(dirname).resolve().joinpath(filename))
    return files


def register(
    pkg_root: str,
    disable_auto_version: bool = False,
    remote: bool = False,
):
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


    Example:
        >>> register("./example_workflow")

    .. _Flyte:
        https://docs.flyte.org
    .. _flytekit documentation:
        https://docs.flyte.org/en/latest/concepts/registration.html
    """

    pkg_root = Path(pkg_root).resolve()
    with _CentromereCtx(
        pkg_root,
        disable_auto_version=disable_auto_version,
        remote=remote,
    ) as ctx:
        print(f"Initializing registration for {pkg_root}")
        if remote:
            print("Connecting to remote server for docker build [alpha]...")

        with contextlib.ExitStack() as stack:
            td = stack.enter_context(
                _TmpDir(
                    ssh_client=ctx.ssh_client,
                    remote=remote,
                )
            )
            _build_and_serialize(
                ctx,
                ctx.default_container.image_name,
                ctx.default_container.dockerfile.parent,
                td,
            )
            protos = _recursive_list(td)
            if remote:
                local_td = stack.enter_context(tempfile.TemporaryDirectory())
                scp = SCPClient(ctx.ssh_client.get_transport(), sanitize=lambda x: x)
                scp.get(f"{td}/*", local_path=local_td, recursive=True)
                protos = _recursive_list(local_td)
            else:
                protos = _recursive_list(td)

            for task_name, container in ctx.container_map.items():
                task_td = stack.enter_context(
                    _TmpDir(ssh_client=ctx.ssh_client, remote=remote)
                )
                try:
                    _build_and_serialize(
                        ctx,
                        container.image_name,
                        # always use root as build context
                        ctx.default_container.dockerfile.parent,
                        task_td,
                        dockerfile=container.dockerfile,
                    )

                    if remote:
                        local_td = stack.enter_context(tempfile.TemporaryDirectory())
                        scp = SCPClient(
                            ctx.ssh_client.get_transport(),
                            sanitize=lambda x: x,
                        )
                        scp.get(f"{task_td}/*", local_path=local_td, recursive=True)
                        new_protos = _recursive_list(local_td)
                    else:
                        new_protos = _recursive_list(task_td)
                    try:
                        split_task_name = task_name.split(".")
                        task_name = ".".join(
                            split_task_name[split_task_name.index("wf") :]
                        )
                        for new_proto in new_protos:
                            if task_name in new_proto.name:
                                protos = [
                                    new_proto if new_proto.name == f.name else f
                                    for f in protos
                                ]
                    except ValueError as e:
                        raise ValueError(
                            f"Unable to match {task_name} to any of the protobuf files"
                            f" in {new_protos}"
                        ) from e
                except TypeError as e:
                    raise ValueError(
                        "The path to your provided dockerfile ",
                        f"{container.dockerfile} given to {task_name} is invalid.",
                    ) from e

            reg_resp = _register_serialized_pkg(ctx, protos)
            _print_reg_resp(reg_resp, ctx.default_container.image_name)
