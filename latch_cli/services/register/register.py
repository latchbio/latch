import contextlib
import re
import shutil
import sys
import tempfile
import time
import webbrowser
from pathlib import Path
from typing import Iterable, List, Optional

import click
import gql
import latch_sdk_gql.execute as l_gql
from flytekit.core.workflow import WorkflowBase
from scp import SCPClient

from latch.utils import current_workspace, get_workspaces
from latch_cli.centromere.ctx import _CentromereCtx
from latch_cli.centromere.utils import MaybeRemoteDir
from latch_cli.constants import latch_constants
from latch_cli.services.register.constants import ANSI_REGEX, MAX_LINES
from latch_cli.services.register.utils import (
    DockerBuildLogItem,
    build_image,
    register_serialized_pkg,
    serialize_pkg_in_container,
    upload_image,
)
from latch_cli.utils import WorkflowType


def _delete_lines(num: int):
    """Deletes the previous len(lines) lines, assuming cursor is on a
    new line just below the first line to be deleted"""
    for i in range(num):
        click.echo("\x1b[1F\x1b[0G\x1b[2K", nl=False)


def _print_window(cur_lines: List[str], line: str):
    """Prints the lines curr_lines[1:] and line, overwriting curr_lines
    in the process"""
    if line == "":
        return cur_lines
    elif len(cur_lines) >= MAX_LINES:
        line = ANSI_REGEX.sub("", line)
        new_lines = cur_lines[len(cur_lines) - MAX_LINES + 1 :]
        new_lines.append(line)
        _delete_lines(len(cur_lines))
        for s in new_lines:
            click.echo("\x1b[38;5;245m" + s + "\x1b[0m")
        return new_lines
    else:
        click.echo("\x1b[38;5;245m" + line + "\x1b[0m")
        cur_lines.append(line)
        return cur_lines


docker_build_step_pat = re.compile("^Step [0-9]+/[0-9]+ :")


def print_and_write_build_logs(
    build_logs: Iterable[DockerBuildLogItem],
    image: str,
    pkg_root: Path,
    *,
    progress_plain: bool = False,
):
    click.secho(f"Building Docker image", bold=True)

    logs_path = pkg_root / ".latch" / ".logs" / image
    logs_path.mkdir(parents=True, exist_ok=True)

    click.echo(f"  Writing log to {click.style(logs_path, italic=True)}\n")

    with (logs_path / "docker-build-logs.txt").open("w") as save_file:
        cur_lines: list[str] = []

        for x in build_logs:
            # for dockerfile parse errors
            message = x.get("message")
            if message is not None:
                save_file.write(f"{message}\n")
                raise ValueError(message)

            lines = x.get("stream")
            error = x.get("error")
            if error is not None:
                save_file.write(f"{error}\n")
                click.secho(f"Error when building image:\n{error}", fg="red", bold=True)
                sys.exit(1)

            if lines is not None:
                save_file.write(f"{lines}\n")

                if not progress_plain:
                    for line in lines.split("\n"):
                        curr_terminal_width = shutil.get_terminal_size()[0]

                        if len(line) > curr_terminal_width:
                            line = line[: curr_terminal_width - 3] + "..."

                        if docker_build_step_pat.match(line):
                            _delete_lines(len(cur_lines))
                            cur_lines = []
                            click.secho(line, fg="blue")
                        else:
                            cur_lines = _print_window(cur_lines, line)
                else:
                    click.echo(lines, nl=False)

        if not progress_plain:
            _delete_lines(len(cur_lines))


def print_upload_logs(upload_image_logs, image):
    click.secho(f"Uploading Docker image", bold=True)
    prog_map = {}

    def _pp_prog_map(prog_map, prev_lines):
        if prev_lines > 0:
            click.echo("\x1b[2K\x1b[1E" * prev_lines + f"\x1b[{prev_lines}F", nl=False)
        prog_chunk = ""
        i = 0
        for id, prog in prog_map.items():
            if prog is None:
                continue
            prog_chunk += f"{id} ~ {prog}\n"
            i += 1
        if prog_chunk == "":
            return 0
        click.echo(prog_chunk + f"\x1b[{i}A", nl=False)
        return i

    prev_lines = 0

    for x in upload_image_logs:
        if (
            x.get("error") is not None
            and "denied: Your authorization token has expired." in x["error"]
        ):
            click.secho(
                f"\nDocker authorization token for {image} is expired.",
                fg="red",
                bold=True,
            )
            sys.exit(1)

        prog_map[x.get("id")] = x.get("progress")
        prev_lines = _pp_prog_map(prog_map, prev_lines)


def _print_reg_resp(resp, image):
    click.secho(f"Registering workflow {image}", bold=True)
    version = image.split(":")[1]

    if not resp.get("success"):
        error_str = f"Failed:\n\n"
        if resp.get("stderr") is not None:
            for line in resp.get("stderr").split("\n"):
                if not line:
                    continue

                if line.startswith('{"json"'):
                    continue

                error_str += line + "\n"

        if "task with different structure already exists" in error_str:
            error_str = (
                f"Version {version} already exists. Make sure that you've saved any"
                " changes you made."
            )

        click.secho(f"\n{error_str}", fg="red", bold=True)
        sys.exit(1)
    elif not "Successfully registered file" in resp["stdout"]:
        click.secho(
            f"\nVersion ({version}) already exists."
            " Make sure that you've saved any changes you made.",
            fg="red",
            bold=True,
        )
        sys.exit(1)

    click.echo(resp.get("stdout"))


def print_serialize_logs(serialize_logs, image):
    click.secho(f"\nSerializing workflow", bold=True)
    for x in serialize_logs:
        click.echo(x, nl=False)


def _build_and_serialize(
    ctx: _CentromereCtx,
    image_name: str,
    context_path: Path,
    tmp_dir: str,
    dockerfile: Optional[Path] = None,
    *,
    progress_plain: bool = False,
    sm_jit_wf: Optional[WorkflowBase] = None,
):
    assert ctx.pkg_root is not None

    image_build_logs = build_image(ctx, image_name, context_path, dockerfile)
    print_and_write_build_logs(
        image_build_logs, image_name, ctx.pkg_root, progress_plain=progress_plain
    )

    if ctx.workflow_type == WorkflowType.snakemake:
        from ...snakemake.serialize import (
            JITRegisterWorkflow,
            serialize_jit_register_workflow,
        )

        assert sm_jit_wf is not None and isinstance(sm_jit_wf, JITRegisterWorkflow)
        assert ctx.dkr_repo is not None

        serialize_jit_register_workflow(sm_jit_wf, tmp_dir, image_name, ctx.dkr_repo)
    else:
        serialize_logs, container_id = serialize_pkg_in_container(
            ctx, image_name, tmp_dir, ctx.workflow_name
        )
        print_serialize_logs(serialize_logs, image_name)

        assert ctx.dkr_client is not None
        exit_status = ctx.dkr_client.wait(container_id)
        if exit_status["StatusCode"] != 0:
            click.secho("\nWorkflow failed to serialize", fg="red", bold=True)
            sys.exit(1)

        ctx.dkr_client.remove_container(container_id)

    click.echo()
    upload_image_logs = upload_image(ctx, image_name)
    print_upload_logs(upload_image_logs, image_name)


def _recursive_list(directory: Path) -> List[Path]:
    res: List[Path] = []

    stack: List[Path] = [directory]
    while len(stack) > 0:
        cur = stack.pop()
        for x in cur.iterdir():
            res.append(x)

            if x.is_dir():
                stack.append(x)

    return res


def register(
    pkg_root: str,
    *,
    disable_auto_version: bool = False,
    remote: bool = False,
    open: bool = False,
    skip_confirmation: bool = False,
    metadata_root: Optional[Path] = None,
    snakefile: Optional[Path] = None,
    nf_script: Optional[Path] = None,
    nf_execution_profile: Optional[str] = None,
    progress_plain: bool = False,
    cache_tasks: bool = False,
    use_new_centromere: bool = False,
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

    if snakefile is not None:
        try:
            import snakemake
        except ImportError as e:
            click.secho("\n`snakemake` package is not installed.", fg="red", bold=True)
            sys.exit(1)

    with _CentromereCtx(
        Path(pkg_root),
        disable_auto_version=disable_auto_version,
        remote=remote,
        metadata_root=metadata_root,
        snakefile=snakefile,
        nf_script=nf_script,
        use_new_centromere=use_new_centromere,
    ) as ctx:
        assert ctx.workflow_name is not None, "Unable to determine workflow name"
        assert ctx.version is not None, "Unable to determine workflow version"

        # todo(maximsmol): we really want the workflow display name here
        click.echo(
            " ".join(
                [click.style("Workflow name:", fg="bright_blue"), ctx.workflow_name]
            )
        )
        click.echo(" ".join([click.style("Version:", fg="bright_blue"), ctx.version]))

        workspaces = get_workspaces()
        ws_name = next(
            (x[1]["name"] for x in workspaces.items() if x[0] == current_workspace()),
            "N/A",
        )
        click.echo(
            " ".join([
                click.style("Target workspace:", fg="bright_blue"),
                ws_name,
                f"({current_workspace()})",
            ])
        )
        click.echo(
            " ".join([
                click.style("Workflow root:", fg="bright_blue"),
                str(ctx.default_container.pkg_dir),
            ])
        )

        click.echo(
            " ".join([
                click.style("Workflow type:", fg="bright_blue"),
                ctx.workflow_type.value,
            ])
        )
        if ctx.workflow_type == WorkflowType.snakemake:
            click.echo(
                " ".join(
                    [click.style("Snakefile:", fg="bright_blue"), str(ctx.snakefile)]
                )
            )
        elif ctx.workflow_type == WorkflowType.nextflow:
            click.echo(
                " ".join(
                    [click.style("NF script:", fg="bright_blue"), str(ctx.nf_script)]
                )
            )

        if use_new_centromere:
            click.secho("Using experimental registration server.", fg="yellow")

        if not skip_confirmation:
            if not click.confirm("Start registration?"):
                click.secho("Cancelled", bold=True)
                return
        else:
            click.secho("Skipping confirmation because of --yes", bold=True)

        sm_jit_wf = None
        if ctx.workflow_type == WorkflowType.snakemake:
            assert ctx.snakefile is not None
            assert ctx.version is not None

            from ...snakemake.serialize import generate_jit_register_code
            from ...snakemake.workflow import build_jit_register_wrapper

            sm_jit_wf = build_jit_register_wrapper(cache_tasks)
            generate_jit_register_code(
                sm_jit_wf,
                ctx.pkg_root,
                ctx.metadata_root,
                ctx.snakefile,
                ctx.version,
                ctx.default_container.image_name,
                current_workspace(),
            )
        elif ctx.workflow_type == WorkflowType.nextflow:
            assert ctx.nf_script is not None
            assert ctx.pkg_root is not None

            from ...nextflow.dependencies import ensure_nf_dependencies
            from ...nextflow.workflow import generate_nextflow_workflow

            ensure_nf_dependencies(ctx.pkg_root)

            dest = ctx.pkg_root / "wf" / "entrypoint.py"
            dest.parent.mkdir(exist_ok=True)
            generate_nextflow_workflow(
                ctx.pkg_root,
                ctx.metadata_root,
                ctx.nf_script,
                dest,
                execution_profile=nf_execution_profile,
            )

        click.secho("\nInitializing registration", bold=True)
        transport = None
        scp = None

        click.echo(
            " ".join([
                click.style("Docker Image:", fg="bright_blue"),
                ctx.default_container.image_name,
            ])
        )
        click.echo()

        if remote:
            click.secho("Connecting to remote server for docker build\n", bold=True)

            assert ctx.ssh_client is not None
            transport = ctx.ssh_client.get_transport()

            assert transport is not None
            scp = SCPClient(transport=transport, sanitize=lambda x: x)

        with contextlib.ExitStack() as stack:
            # We serialize locally with snakemake projects
            remote_dir_client = None
            if snakefile is None:
                remote_dir_client = ctx.ssh_client
            td: str = stack.enter_context(MaybeRemoteDir(remote_dir_client))
            _build_and_serialize(
                ctx,
                ctx.default_container.image_name,
                ctx.default_container.pkg_dir,
                td,
                dockerfile=ctx.default_container.dockerfile,
                progress_plain=progress_plain,
                sm_jit_wf=sm_jit_wf,
            )

            if remote and snakefile is None:
                local_td = Path(stack.enter_context(tempfile.TemporaryDirectory()))

                assert scp is not None
                scp.get(f"{td}/*", local_path=str(local_td), recursive=True)
            else:
                local_td = Path(td)

            protos = _recursive_list(local_td)

            for task_name, container in ctx.container_map.items():
                task_td = stack.enter_context(MaybeRemoteDir(ctx.ssh_client))
                try:
                    _build_and_serialize(
                        ctx,
                        container.image_name,
                        ctx.default_container.pkg_dir,
                        task_td,
                        dockerfile=container.dockerfile,
                        progress_plain=progress_plain,
                    )

                    if remote:
                        local_task_td = Path(
                            stack.enter_context(tempfile.TemporaryDirectory())
                        )

                        assert scp is not None
                        scp.get(
                            f"{task_td}/*", local_path=str(local_td), recursive=True
                        )

                        new_protos = _recursive_list(local_td)
                    else:
                        local_task_td = Path(task_td)

                    new_protos = _recursive_list(local_task_td)

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

            reg_resp = register_serialized_pkg(
                protos, ctx.token, ctx.version, current_workspace()
            )
            _print_reg_resp(reg_resp, ctx.default_container.image_name)

            click.secho("Successfully registered workflow.", fg="green", bold=True)

            wf_infos = []
            retries = 0

            wf_name = ctx.workflow_name

            name_path = Path(pkg_root) / latch_constants.pkg_workflow_name
            if not name_path.exists():
                name_path.write_text(ctx.workflow_name)

            while len(wf_infos) == 0:
                wf_infos = l_gql.execute(
                    gql.gql("""
                    query workflowQuery($name: String, $ownerId: BigInt, $version: String) {
                        workflowInfos(condition: { name: $name, ownerId: $ownerId, version: $version}) {
                            nodes {
                                id
                            }
                        }
                    }
                    """),
                    {
                        "name": wf_name,
                        "version": ctx.version,
                        "ownerId": current_workspace(),
                    },
                )["workflowInfos"]["nodes"]
                time.sleep(1)

                if retries >= 5:
                    click.secho(
                        "Failed to query workflow ID in 5 seconds.", fg="red", bold=True
                    )
                    click.secho(
                        "This could be due to high demand or a bug in the platform.",
                        fg="red",
                    )
                    click.secho(
                        "If the workflow is not visible in latch console, contact"
                        " support.",
                        fg="red",
                    )
                    break

                retries += 1

            if len(wf_infos) > 0:
                if len(wf_infos) > 1:
                    click.secho(
                        f"Workflow {ctx.workflow_name}:{ctx.version} is not unique."
                        " The link below might be wrong.",
                        fg="yellow",
                    )

                wf_id = wf_infos[0]["id"]
                url = f"https://console.latch.bio/workflows/{wf_id}"
                click.secho(url, fg="green")

                if open:
                    webbrowser.open(url)
