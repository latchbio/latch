"""Entrypoints to service functions through a latch_cli."""

import os
import sys
from pathlib import Path
from textwrap import dedent
from typing import Callable, List, Optional, Tuple, TypeVar, Union

import click
from packaging.version import parse as parse_version
from typing_extensions import ParamSpec

import latch_cli.click_utils
from latch_cli.click_utils import EnumChoice
from latch_cli.exceptions.handler import CrashHandler
from latch_cli.services.cp.autocomplete import complete as cp_complete
from latch_cli.services.cp.autocomplete import remote_complete
from latch_cli.services.cp.config import Progress
from latch_cli.services.init.init import template_flag_to_option
from latch_cli.services.local_dev import TaskSize
from latch_cli.utils import (
    AuthenticationError,
    WorkflowType,
    get_auth_header,
    get_latest_package_version,
    get_local_package_version,
)
from latch_cli.workflow_config import BaseImageOptions

latch_cli.click_utils.patch()

crash_handler = CrashHandler()

P = ParamSpec("P")
T = TypeVar("T")


def requires_login(f: Callable[P, T]) -> Callable[P, T]:
    def decorated(*args, **kwargs):
        try:
            get_auth_header()
        except AuthenticationError as e:
            click.secho(
                dedent("""
                Unable to authenticate with Latch.

                If you are on a machine with a browser, run `latch login`.

                If not, navigate to `https://console.latch.bio/settings/developer` on a different machine, select `Access Tokens`, and copy your `API Key` to `~/.latch/token` on this machine.
                If you do not see this value in the console, make sure you are logged into your personal workspace.
                """).strip("\n"),
                fg="red",
            )
            raise click.exceptions.Exit(1) from e

        return f(*args, **kwargs)

    return decorated


@click.group(
    "latch",
    context_settings={
        "max_content_width": 160,
    },
)
@click.version_option(package_name="latch")
def main():
    """
    Collection of command line tools for using the Latch SDK and
    interacting with the Latch platform.
    """
    if os.environ.get("LATCH_SKIP_VERSION_CHECK") is not None:
        return

    local_ver = parse_version(get_local_package_version())
    latest_ver = parse_version(get_latest_package_version())
    if local_ver < latest_ver:
        click.secho(
            dedent(f"""
                WARN: Your local version of latch ({local_ver}) is out of date. This may result in unexpected behavior.
                Please upgrade to the latest version ({latest_ver}) using `python3 -m pip install --upgrade latch`.
                """).strip("\n"),
            fg="yellow",
        )

    crash_handler.init()


@main.command("dockerfile")
@click.argument("pkg_root", type=click.Path(exists=True, file_okay=False))
@click.option(
    "-s",
    "--snakemake",
    is_flag=True,
    default=False,
    type=bool,
    help="Generate a Dockerfile with arguments needed for Snakemake compatability",
)
def dockerfile(pkg_root: str, snakemake: bool = False):
    """Generates a user editable dockerfile for a workflow and saves under `pkg_root/Dockerfile`.

    Visit docs.latch.bio to learn more.
    """

    crash_handler.message = "Failed to generate Dockerfile."
    crash_handler.pkg_root = pkg_root

    from latch_cli.docker_utils import generate_dockerfile

    source = Path(pkg_root)
    dest = source / "Dockerfile"
    if dest.exists() and not click.confirm(
        f"Dockerfile already exists at `{dest}`. Overwrite?"
    ):
        return
    workflow_type = WorkflowType.latchbiosdk
    if snakemake is True:
        workflow_type = WorkflowType.snakemake
    generate_dockerfile(source, dest, wf_type=workflow_type)

    click.secho(f"Successfully generated dockerfile `{dest}`", fg="green")


@main.command("register")
@click.argument("pkg_root", type=click.Path(exists=True, file_okay=False))
@click.option(
    "-d",
    "--disable-auto-version",
    is_flag=True,
    default=False,
    type=bool,
    help=(
        "Whether to automatically bump the version of the workflow each time register"
        " is called."
    ),
)
@click.option(
    "--remote/--no-remote",
    is_flag=True,
    default=True,
    type=bool,
    help="Use a remote server to build workflow.",
)
@click.option(
    "--docker-progress",
    type=click.Choice(["plain", "tty", "auto"], case_sensitive=False),
    default="auto",
    help=(
        "`tty` shows only the last N lines of the build log. `plain` does no special"
        " handling. `auto` chooses `tty` when stdout is a terminal and `plain`"
        " otherwise. Equivalent to Docker's `--progress` flag."
    ),
)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    default=False,
    type=bool,
    help="Skip the confirmation dialog.",
)
@click.option(
    "--snakefile",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to a Snakefile to register.",
)
@click.option(
    "--cache-tasks/--no-cache-tasks",
    "-c/-C",
    is_flag=True,
    default=False,
    type=bool,
    help=(
        "Whether or not to cache snakemake tasks. Ignored if --snakefile is not"
        " provided."
    ),
)
@requires_login
def register(
    pkg_root: str,
    disable_auto_version: bool,
    remote: bool,
    docker_progress: str,
    yes: bool,
    snakefile: Optional[Path],
    cache_tasks: bool,
):
    """Register local workflow code to Latch.

    Visit docs.latch.bio to learn more.
    """

    use_new_centromere = os.environ.get("LATCH_REGISTER_BETA") is not None

    crash_handler.message = "Unable to register workflow."
    crash_handler.pkg_root = pkg_root

    from latch_cli.services.register import register

    register(
        pkg_root,
        disable_auto_version=disable_auto_version,
        remote=remote,
        skip_confirmation=yes,
        snakefile=snakefile,
        progress_plain=(docker_progress == "auto" and not sys.stdout.isatty())
        or docker_progress == "plain",
        use_new_centromere=use_new_centromere,
        cache_tasks=cache_tasks,
    )


@main.command("develop")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    type=bool,
    help="Skip the confirmation dialog.",
)
@click.option(
    "--image",
    "-i",
    type=str,
    help="Image to use for develop session.",
)
@click.option(
    "--size",
    "-s",
    type=EnumChoice(TaskSize, case_sensitive=False),
    help="Instance size to use for develop session.",
)
@requires_login
def local_development(
    pkg_root: Path, yes: bool, image: Optional[str], size: Optional[TaskSize]
):
    """Develop workflows "locally"

    Visit docs.latch.bio to learn more.
    """

    crash_handler.message = "Error during local development session"
    crash_handler.pkg_root = str(pkg_root)

    if os.environ.get("LATCH_DEVELOP_BETA") is not None:
        from latch_cli.services.local_dev import local_development

        local_development(
            pkg_root.resolve(), skip_confirm_dialog=yes, size=size, image=image
        )
    else:
        from latch_cli.services.local_dev_old import local_development

        local_development(pkg_root.resolve())


@main.command("login")
@click.option(
    "--connection",
    type=str,
    default=None,
    help="Specific AuthO connection name e.g. for SSO.",
)
def login(connection: Optional[str]):
    """Manually login to Latch."""

    crash_handler.message = "Unable to log in"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.login import login

    login(connection)
    click.secho("Successfully logged into LatchBio.", fg="green")


@main.command("init")
@click.argument("pkg_name", nargs=1)
@click.option(
    "--template",
    "-t",
    type=click.Choice(
        list(template_flag_to_option.keys()),
        case_sensitive=False,
    ),
)
@click.option(
    "--dockerfile",
    "-d",
    help="Create a user editable Dockerfile for this workflow.",
    is_flag=True,
    default=False,
)
@click.option(
    "--base-image",
    "-b",
    help="Which base image to use for the Dockerfile.",
    type=click.Choice(
        list(BaseImageOptions._member_names_),
        case_sensitive=False,
    ),
    default="default",
)
def init(
    pkg_name: str,
    template: Optional[str] = None,
    dockerfile: bool = False,
    base_image: str = "default",
):
    """Initialize boilerplate for local workflow code."""

    crash_handler.message = f"Unable to initialize {pkg_name}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.init import init

    created = init(pkg_name, template, dockerfile, base_image)
    if created:
        click.secho(f"Created a latch workflow in `{pkg_name}`", fg="green")
        click.secho("Run", fg="green")
        click.secho(f"\t$ latch register {pkg_name}", fg="green")
        click.secho("To register the workflow with console.latch.bio.", fg="green")
        return

    click.secho("No workflow created.", fg="yellow")


@main.command("cp")
@click.argument("src", shell_complete=cp_complete, nargs=-1)
@click.argument("dest", shell_complete=cp_complete, nargs=1)
@click.option(
    "--progress",
    help="Type of progress information to show while copying",
    type=EnumChoice(Progress, case_sensitive=False),
    default="tasks",
    show_default=True,
)
@click.option(
    "--verbose",
    "-v",
    help="Print file names as they are copied",
    is_flag=True,
    default=False,
    show_default=True,
)
@click.option(
    "--no-glob",
    "-G",
    help="Don't expand globs in remote paths",
    is_flag=True,
    default=False,
    show_default=True,
)
@requires_login
def cp(
    src: List[str],
    dest: str,
    progress: Progress,
    verbose: bool,
    no_glob: bool,
):
    """Copy local files to LatchData and vice versa."""

    crash_handler.message = f"Unable to copy {src} to {dest}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.cp.main import cp

    cp(
        src,
        dest,
        progress=progress,
        verbose=verbose,
        expand_globs=not no_glob,
    )


@main.command("mv")
@click.argument("src", shell_complete=remote_complete, nargs=1)
@click.argument("dest", shell_complete=remote_complete, nargs=1)
@click.option(
    "--no-glob",
    "-G",
    help="Don't expand globs in remote paths",
    is_flag=True,
    default=False,
    show_default=True,
)
@requires_login
def mv(src: str, dest: str, no_glob: bool):
    """Move remote files in LatchData."""

    crash_handler.message = f"Unable to move {src} to {dest}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.move import move

    move(src, dest, no_glob=no_glob)


@main.command("ls")
@click.option(
    "--group-directories-first",
    "--gdf",
    help="List directories before files.",
    is_flag=True,
    default=False,
)
@click.argument("paths", nargs=-1, shell_complete=remote_complete)
@requires_login
def ls(paths: Tuple[str], group_directories_first: bool):
    """
    List the contents of a Latch Data directory
    """

    crash_handler.message = f"Unable to display contents of {paths}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.ls import ls

    # If the user doesn't provide any arguments, default to root
    if len(paths) == 0:
        paths = ("/",)

    for path in paths:
        if len(paths) > 1:
            click.echo(f"{path}:")

        ls(
            path,
            group_directories_first=group_directories_first,
        )

        if len(paths) > 1:
            click.echo("")


@main.command("generate-metadata")
@click.argument("config_file", nargs=1, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help=(
        "Overwrite an existing `latch_metadata/parameters.py` file without confirming."
    ),
)
@click.option(
    "--no-infer-files",
    "-I",
    is_flag=True,
    default=False,
    help="Don't parse strings with common file extensions as file parameters.",
)
@click.option(
    "--no-defaults",
    "-D",
    is_flag=True,
    default=False,
    help="Don't generate defaults for parameters.",
)
def generate_metadata(
    config_file: Path, yes: bool, no_infer_files: bool, no_defaults: bool
):
    """Generate a `latch_metadata.py` file from a Snakemake config file"""

    from latch_cli.snakemake.config.parser import generate_metadata

    generate_metadata(
        config_file,
        skip_confirmation=yes,
        infer_files=not no_infer_files,
        generate_defaults=not no_defaults,
    )


@main.command("launch")
@click.argument("params_file", nargs=1, type=click.Path(exists=True))
@click.option(
    "--version",
    default=None,
    help="The version of the workflow to launch. Defaults to latest.",
)
@requires_login
def launch(params_file: Path, version: Union[str, None] = None):
    """Launch a workflow using a python parameter map."""

    crash_handler.message = f"Unable to launch workflow"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.launch import launch

    wf_name = launch(params_file, version)
    if version is None:
        version = "latest"

    click.secho(
        f"Successfully launched workflow named {wf_name} with version {version}.",
        fg="green",
    )


@main.command("get-params")
@click.argument("wf_name", nargs=1)
@click.option(
    "--version",
    default=None,
    help="The version of the workflow. Defaults to latest.",
)
@requires_login
def get_params(wf_name: Union[str, None], version: Union[str, None] = None):
    """Generate a python parameter map for a workflow."""
    crash_handler.message = "Unable to generate param map for workflow"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.get_params import get_params

    get_params(wf_name, version)
    if version is None:
        version = "latest"
    click.secho(
        f"Successfully generated python param map named {wf_name}.params.py with"
        f" version {version}\n Run `latch launch {wf_name}.params.py` to launch it.",
        fg="green",
    )


@main.command("get-wf")
@click.option(
    "--name",
    default=None,
    help="The name of the workflow to list. Will display all versions",
)
@requires_login
def get_wf(name: Union[str, None] = None):
    """List workflows."""
    crash_handler.message = "Unable to get workflows"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.get import get_wf

    wfs = get_wf(name)
    id_padding, name_padding, version_padding = 0, 0, 0
    for wf in wfs:
        id, name, version = wf
        id_len, name_len, version_len = len(str(id)), len(name), len(version)
        id_padding = max(id_padding, id_len)
        name_padding = max(name_padding, name_len)
        version_padding = max(version_padding, version_len)

    # TODO(ayush): make this much better
    click.secho(
        f"ID{id_padding * ' '}\tName{name_padding * ' '}\tVersion{version_padding * ' '}"
    )
    for wf in wfs:
        click.secho(
            f"{wf[0]}{(id_padding - len(str(wf[0]))) * ' '}\t{wf[1]}{(name_padding - len(wf[1])) * ' '}\t{wf[2]}{(version_padding - len(wf[2])) * ' '}"
        )


@main.command("open")
@click.argument("remote_file", nargs=1, type=str)
@requires_login
def open_remote_file(remote_file: str):
    """Open a remote file in the browser."""
    crash_handler.message = f"Unable to open {remote_file}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.open_file import open_file

    open_file(remote_file)
    click.secho(f"Successfully opened {remote_file}.", fg="green")


@main.command("rm")
@click.argument("remote_path", nargs=1, type=str)
@requires_login
def rm(remote_path: str):
    """Deletes a remote entity."""
    crash_handler.message = f"Unable to delete {remote_path}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.deprecated.rm import rm

    click.secho(
        f"Warning: `latch rm` is deprecated and will be removed soon.", fg="yellow"
    )
    rm(remote_path)
    click.secho(f"Successfully deleted {remote_path}.", fg="green")


@main.command("mkdir")
@click.argument("remote_directory", nargs=1, type=str)
@requires_login
def mkdir(remote_directory: str):
    """Creates a new remote directory."""
    crash_handler.message = f"Unable to create directory {remote_directory}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.deprecated.mkdir import mkdir

    click.secho(
        "Warning: `latch mkdir` is deprecated and will be removed soon.",
        fg="yellow",
    )
    mkdir(remote_directory)
    click.secho(f"Successfully created directory {remote_directory}.", fg="green")


@main.command("touch")
@click.argument("remote_file", nargs=1, type=str)
@requires_login
def touch(remote_file: str):
    """Creates an empty text file."""
    crash_handler.message = f"Unable to create {remote_file}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.deprecated.touch import touch

    click.secho(
        "Warning: `latch touch` is deprecated and will be removed soon.",
        fg="yellow",
    )
    touch(remote_file)
    click.secho(f"Successfully touched {remote_file}.", fg="green")


@main.command("exec")
@click.argument("task_name", nargs=1, type=str)
@requires_login
def execute(task_name: str):
    """Drops the user into an interactive shell from within a task."""
    crash_handler.message = f"Unable to exec into {task_name}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.execute import execute

    execute(task_name)


@main.command("preview")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True, path_type=Path))
@requires_login
def preview(pkg_root: Path):
    """Creates a preview of your workflow interface."""
    crash_handler.message = f"Unable to preview inputs for {pkg_root}"
    crash_handler.pkg_root = str(pkg_root)

    from latch_cli.services.preview import preview

    preview(pkg_root)


@main.command("workspace")
@requires_login
def workspace():
    """Spawns an interactive terminal prompt allowing users to choose what workspace they want to work in."""

    crash_handler.message = "Unable to fetch workspaces"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.workspace import workspace

    workspace()


@main.command("get-executions")
@requires_login
def get_executions():
    """Spawns an interactive terminal UI that shows all executions in a given workspace"""

    crash_handler.message = "Unable to fetch executions"

    from latch_cli.services.get_executions import get_executions

    get_executions()


@main.group()
def pods():
    """Manage pods"""
    pass


@pods.command("stop")
@click.argument("pod_id", nargs=1, type=int, required=False)
@requires_login
def stop_pod(pod_id: Optional[int] = None):
    """Stops a pod given a pod_id or the pod from which the command is run"""
    crash_handler.message = "Unable to stop pod"

    from latch_cli.services.stop_pod import stop_pod

    if pod_id is None:
        id_path = Path("/root/.latch/id")

        try:
            pod_id = int(id_path.read_text().strip("\n"))
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                err_str = f"Pod ID not found at `{id_path}`"
            elif isinstance(e, ValueError):
                err_str = f"Could not parse Pod ID at `{id_path}`"
            else:
                err_str = f"Error reading Pod ID from `{id_path}`"

            click.secho(
                f"{err_str} -- please provide a Pod ID as a command line argument.",
                fg="red",
            )
            return

    stop_pod(pod_id)


# Test data subcommands.


@main.group(invoke_without_command=True)
@click.version_option(package_name="latch")
@click.pass_context
def test_data(ctx: click.Context):
    """Subcommands to upload and delete test data objects."""
    if ctx.invoked_subcommand is None:
        click.secho(f"{ctx.get_help()}")


@test_data.command("upload")
@click.argument("src_path", nargs=1, type=click.Path(exists=True))
@click.option(
    "--dont-confirm-overwrite",
    "-d",
    is_flag=True,
    default=False,
    type=bool,
    help="Automatically overwrite any files without asking for confirmation.",
)
@requires_login
def test_data_upload(src_path: str, dont_confirm_overwrite: bool):
    """Upload test data object."""

    crash_handler.message = f"Unable to upload {src_path} to managed bucket"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.test_data.upload import upload

    s3_url = upload(src_path, dont_confirm_overwrite)
    click.secho(f"Successfully uploaded to {s3_url}", fg="green")


@test_data.command("remove")
@click.argument("object_url", nargs=1, type=str)
@requires_login
def test_data_remove(object_url: str):
    """Remove test data object."""

    crash_handler.message = f"Unable to remove {object_url} from managed bucket"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.test_data.remove import remove

    remove(object_url)
    click.secho(f"Successfully removed {object_url}", fg="green")


@test_data.command("ls")
@requires_login
def test_data_ls():
    """List test data objects."""

    crash_handler.message = f"Unable to list objects within managed bucket"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.test_data.ls import ls

    objects = ls()
    click.secho("Listing your managed objects by full S3 path.\n", fg="green")
    for o in objects:
        print(f"\ts3://latch-public/{o}")


@main.command("sync")
@click.argument("srcs", nargs=-1)
@click.argument("dst", nargs=1)
@click.option(
    "--delete",
    help="Delete extraneous files from destination.",
    is_flag=True,
    default=False,
)
@click.option(
    "--ignore-unsyncable",
    help=(
        "Synchronize even if some source paths do not exist or refer to special files."
    ),
    is_flag=True,
    default=False,
)
@requires_login
def sync(srcs: List[str], dst: str, delete: bool, ignore_unsyncable: bool):
    """
    Update the contents of a remote directory with local data or vice versa.
    """
    from latch_cli.services.sync import sync

    # todo(maximsmol): remote -> local
    # todo(maximsmol): remote -> remote
    sync(
        srcs,
        dst,
        delete=delete,
        ignore_unsyncable=ignore_unsyncable,
    )
