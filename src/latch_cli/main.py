# ruff: noqa: FBT001, FBT002
"""Entrypoints to service functions through a latch_cli."""

import os
import sys
import traceback
from pathlib import Path
from textwrap import dedent
from typing import Callable, Optional, TypeVar, Union

import click
import gql
from packaging.version import parse as parse_version
from typing_extensions import ParamSpec

import latch_cli.click_utils
from latch.ldata._transfer.progress import Progress as _Progress  # noqa: PLC2701
from latch.utils import current_workspace
from latch_cli.click_utils import EnumChoice
from latch_cli.exceptions.handler import CrashHandler
from latch_cli.services.cp.autocomplete import complete as cp_complete
from latch_cli.services.cp.autocomplete import remote_complete
from latch_cli.services.init.init import template_flag_to_option
from latch_cli.services.k8s.develop import InstanceSize
from latch_cli.utils import (
    AuthenticationError,
    WorkflowType,
    get_auth_header,
    get_latest_package_version,
    get_local_package_version,
    hash_directory,
)
from latch_cli.workflow_config import BaseImageOptions
from latch_sdk_gql.execute import execute as gql_execute

latch_cli.click_utils.patch()

crash_handler = CrashHandler()

P = ParamSpec("P")
T = TypeVar("T")


def requires_login(f: Callable[P, T]) -> Callable[P, T]:
    def decorated(*args: P.args, **kwargs: P.kwargs):
        try:
            get_auth_header()
        except AuthenticationError as e:
            click.secho(
                dedent("""
                Unable to authenticate with Latch.

                If you are on a machine with a browser, run `latch login`.

                If not, navigate to `https://console.latch.bio/settings/developer` on a different machine, select `Access Tokens`, and copy your `User API Key` to `~/.latch/token` on this machine.
                If you do not see this value in the console, make sure you are logged into the correct Latch account.
                """).strip("\n"),
                fg="red",
            )
            raise click.exceptions.Exit(1) from e

        return f(*args, **kwargs)

    decorated.__doc__ = f.__doc__

    return decorated


@click.group("latch", context_settings={"max_content_width": 160})
@click.version_option(package_name="latch")
def main():
    """Collection of command line tools for using the Latch SDK and interacting with the Latch platform."""

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


"""
LOGIN COMMANDS
"""


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


@main.command("workspace")
@requires_login
def workspace():
    """Spawns an interactive terminal prompt allowing users to choose what workspace they want to work in."""

    crash_handler.message = "Unable to fetch workspaces"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.workspace import workspace

    workspace()


"""
WORKFLOW COMMANDS
"""


@main.command("init")
@click.argument("pkg_name", nargs=1)
@click.option(
    "--template",
    "-t",
    type=click.Choice(list(template_flag_to_option.keys()), case_sensitive=False),
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
    type=click.Choice(list(BaseImageOptions._member_names_), case_sensitive=False),
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


@main.command("dockerfile")
@click.argument(
    "pkg_root", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "-s",
    "--snakemake",
    is_flag=True,
    default=False,
    type=bool,
    help="Generate a Dockerfile with arguments needed for Snakemake compatibility",
)
@click.option(
    "-n",
    "--nextflow",
    is_flag=True,
    default=False,
    type=bool,
    help="Generate a Dockerfile with arguments needed for Nextflow compatibility",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    type=bool,
    help="Overwrite existing Dockerfile without confirming",
)
@click.option(
    "-a",
    "--apt-requirements",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to a text file containing apt packages to install.",
)
@click.option(
    "-r",
    "--r-env",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to an environment.R file containing R packages to install.",
)
@click.option(
    "-c",
    "--conda-env",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to an environment.yml file to install via conda.",
)
@click.option(
    "-i",
    "--pyproject",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to a setup.py / buildable pyproject.toml file to install.",
)
@click.option(
    "-p",
    "--pip-requirements",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to a requirements.txt file to install via pip.",
)
@click.option(
    "-d",
    "--direnv",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to a direnv file (.env) containing environment variables to inject into the container.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Where to write the result Dockerfile. Default is Dockerfile in the root of the workflow directory.",
)
@click.option(
    "--config-path",
    type=click.Path(path_type=Path),
    help=(
        "Where to read the config to use for generating the Dockerfile. If a config is not found either at"
        " `config_path` or `config_path / .latch / config`, one will be generated at "
        "`config_path / .latch / config`. If not provided, it will default to the parent of the output Dockerfile"
    ),
)
def dockerfile(
    pkg_root: Path,
    snakemake: bool = False,
    nextflow: bool = False,
    force: bool = False,
    apt_requirements: Optional[Path] = None,
    r_env: Optional[Path] = None,
    conda_env: Optional[Path] = None,
    pyproject: Optional[Path] = None,
    pip_requirements: Optional[Path] = None,
    direnv: Optional[Path] = None,
    output: Optional[Path] = None,
    config_path: Optional[Path] = None,
):
    """Generates a user editable dockerfile for a workflow.

    Visit docs.latch.bio to learn more.
    """

    if snakemake is True and nextflow is True:
        click.secho(
            "Please specify at most one workflow type to generate metadata for. Use"
            " either `--snakemake` or `--nextflow`.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    from latch_cli.docker_utils import DockerfileBuilder, generate_dockerignore
    from latch_cli.workflow_config import get_or_create_workflow_config

    workflow_type = WorkflowType.latchbiosdk
    base_image = BaseImageOptions.default
    if snakemake is True:
        workflow_type = WorkflowType.snakemake
    elif nextflow is True:
        workflow_type = WorkflowType.nextflow
        base_image = BaseImageOptions.nextflow

    if output is None:
        output = pkg_root / "Dockerfile"
    if output.name != "Dockerfile":
        output /= "Dockerfile"

    ignore_path = output.with_name(".dockerignore")

    if config_path is None:
        config_path = output.parent / ".latch" / "config"
    if config_path.name != "config":
        config_path /= ".latch/config"

    click.secho(
        dedent(f"""\
    The following files will be generated:
    {click.style("Dockerfile:", fg="bright_blue")} {output}
    {click.style("Ignore File:", fg="bright_blue")} {ignore_path}
    {click.style("Latch Config:", fg="bright_blue")} {config_path}
    """)
    )

    output.parent.mkdir(exist_ok=True, parents=True)

    # todo(ayush): make overwriting this easier
    config = get_or_create_workflow_config(config_path, base_image_type=base_image)

    builder = DockerfileBuilder(
        pkg_root,
        wf_type=workflow_type,
        config=config,
        apt_requirements=apt_requirements,
        r_env=r_env,
        conda_env=conda_env,
        pyproject=pyproject,
        pip_requirements=pip_requirements,
        direnv=direnv,
    )
    builder.generate(dest=output, overwrite=force)

    generate_dockerignore(ignore_path, wf_type=workflow_type, overwrite=force)


@main.command("generate-metadata")
@click.argument(
    "config_file",
    required=False,
    nargs=1,
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
)
@click.option(
    "--metadata-root",
    type=click.Path(exists=False, path_type=Path, file_okay=False),
    help="Path to directory containing Latch metadata.",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Overwrite an existing `parameters.py` file without confirming.",
)
@click.option(
    "--snakemake",
    "-s",
    is_flag=True,
    default=False,
    type=bool,
    help="Generate Latch metadata for Snakemake.",
)
@click.option(
    "--nextflow",
    "-n",
    is_flag=True,
    default=False,
    type=bool,
    help="Generate Latch metadata for Nextflow.",
)
@click.option(
    "--no-infer-files",
    "-I",
    is_flag=True,
    default=False,
    help=(
        "Don't parse strings with common file extensions as file parameters. Only"
        " supported for Snakemake workflows."
    ),
)
@click.option(
    "--no-defaults",
    "-D",
    is_flag=True,
    default=False,
    help="Don't generate defaults for parameters.",
)
def generate_metadata(
    config_file: Optional[Path],
    metadata_root: Optional[Path],
    snakemake: bool,
    nextflow: bool,
    yes: bool,
    no_infer_files: bool,
    no_defaults: bool,
):
    """Generate a `__init__.py` and `parameters.py` file from a config file"""

    if snakemake is True and nextflow is True:
        click.secho(
            (
                "Please specify only one workflow type to generate metadata for. Use"
                " either `--snakemake` or `--nextflow`."
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if metadata_root is None:
        metadata_root = Path("latch_metadata")

    if nextflow is True:
        from latch_cli.nextflow.config import generate_metadata

        if config_file is None:
            config_file = Path("nextflow_schema.json")

        generate_metadata(config_file, metadata_root, skip_confirmation=yes)
    else:
        from latch_cli.snakemake.config.parser import generate_metadata

        if config_file is None:
            click.secho(
                dedent("""
                Please provide a config file for Snakemake workflows:
                `latch generate-metadata <config_file_path> --snakemake`
                """),
                fg="red",
            )
            raise click.exceptions.Exit(1)

        generate_metadata(
            config_file,
            metadata_root,
            skip_confirmation=yes,
            infer_files=not no_infer_files,
            generate_defaults=not no_defaults,
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
    "--wf-version",
    "-v",
    type=str,
    help="Use the container environment of a specific workflow version",
)
@click.option(
    "--disable-sync",
    "-d",
    is_flag=True,
    default=False,
    type=bool,
    help="Disable the automatic syncing of local files to develop session",
)
@click.option(
    "-s",
    "--snakemake",
    is_flag=True,
    default=False,
    type=bool,
    help="Start a develop session for a Snakemake workflow.",
)
@click.option(
    "--metadata-root",
    type=click.Path(exists=False, path_type=Path, file_okay=False),
    help="Path to directory containing Latch metadata. Only for Snakemake",
)
@click.option(
    "--instance-size",
    "--size",
    "-s",
    type=EnumChoice(InstanceSize, case_sensitive=False),
    default=InstanceSize.small_task,
    help="Size of machine to provision for develop session",
)
@requires_login
def local_development(
    pkg_root: Path,
    yes: bool,
    wf_version: Optional[str],
    disable_sync: bool,
    snakemake: bool,
    metadata_root: Optional[Path],
    instance_size: InstanceSize,
):
    """Develop workflows "locally"

    Visit docs.latch.bio to learn more.
    """

    crash_handler.message = "Error during local development session"
    crash_handler.pkg_root = str(pkg_root)

    # todo(ayush): purge
    if snakemake:
        from latch_cli.services.local_dev_old import local_development

        return local_development(
            pkg_root.resolve(), snakemake, wf_version, metadata_root, disable_sync
        )

    from latch_cli.services.k8s.develop import local_development

    return local_development(
        pkg_root.resolve(),
        skip_confirm_dialog=yes,
        size=instance_size,
        wf_version=wf_version,
        disable_sync=disable_sync,
    )


@main.command("exec")
@click.option(
    "--execution-id", "-e", type=str, help="Optional execution ID to inspect."
)
@click.option("--egn-id", "-g", type=str, help="Optional task execution ID to inspect.")
@click.option(
    "--container-index",
    "-c",
    type=int,
    help="Optional container index to inspect (only used for Map Tasks)",
)
@requires_login
def execute(
    execution_id: Optional[str], egn_id: Optional[str], container_index: Optional[int]
):
    """Drops the user into an interactive shell from within a task."""

    from latch_cli.services.k8s.execute import exec as _exec

    _exec(execution_id=execution_id, egn_id=egn_id, container_index=container_index)


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
    "--open",
    "-o",
    is_flag=True,
    default=False,
    type=bool,
    help="Open the registered workflow in the browser.",
)
@click.option(
    "--mark-as-release",
    "-m",
    is_flag=True,
    default=False,
    type=bool,
    help="Mark the registered workflow as a release.",
)
@click.option(
    "--workflow-module",
    "-w",
    type=str,
    help="Module containing Latch workflow to register. Defaults to `wf`",
)
@click.option(
    "--metadata-root",
    type=click.Path(exists=False, path_type=Path, file_okay=False),
    help="Path to directory containing Latch metadata. Only for Nextflow and Snakemake",
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
@click.option(
    "--nf-script",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to a nextflow script to register.",
)
@click.option(
    "--nf-execution-profile",
    type=str,
    default=None,
    help="Set execution profile for Nextflow workflow",
)
@click.option(
    "--staging",
    is_flag=True,
    default=False,
    type=bool,
    help=(
        "Register the workflow in staging mode - the workflow will not show up in the console but "
        "will be available to develop sessions."
    ),
)
@click.option(
    "--dockerfile",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Path to a custom Dockerfile to use when registering. Default is to (1) use a Dockerfile in "
        "the package root if one exists, or (2) generate one in .latch/Dockerfile if none exists."
    ),
)
@requires_login
def register(
    pkg_root: str,
    disable_auto_version: bool,
    remote: bool,
    docker_progress: str,
    yes: bool,
    open: bool,
    workflow_module: Optional[str],
    metadata_root: Optional[Path],
    snakefile: Optional[Path],
    cache_tasks: bool,
    nf_script: Optional[Path],
    nf_execution_profile: Optional[str],
    mark_as_release: bool,
    staging: bool,
    dockerfile: Optional[Path],
):
    """Register local workflow code to Latch.

    Visit docs.latch.bio to learn more.

    # Exit codes
    1 - Registration failure
    2 - Workflow already registered
    """

    if staging:
        from .services.register.staging import register_staging

        register_staging(
            Path(pkg_root),
            disable_auto_version=disable_auto_version,
            disable_git_version=disable_auto_version,  # todo(ayush): have this apply to normal register too
            remote=remote,
            skip_confirmation=yes,
            wf_module=workflow_module,
            progress_plain=(docker_progress == "auto" and not sys.stdout.isatty())
            or docker_progress == "plain",
            dockerfile_path=dockerfile,
        )

        return

    use_new_centromere = os.environ.get("LATCH_REGISTER_BETA") is not None

    crash_handler.message = "Unable to register workflow."
    crash_handler.pkg_root = pkg_root

    if nf_script is None and (nf_execution_profile is not None):
        click.secho(
            "Command Invalid: --execution-profile flag is only valid when registering a"
            " Nextflow workflow.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    from latch_cli.services.register import register

    register(
        pkg_root,
        disable_auto_version=disable_auto_version,
        remote=remote,
        skip_confirmation=yes,
        open=open,
        wf_module=workflow_module,
        metadata_root=metadata_root,
        snakefile=snakefile,
        nf_script=nf_script,
        nf_execution_profile=nf_execution_profile,
        progress_plain=(docker_progress == "auto" and not sys.stdout.isatty())
        or docker_progress == "plain",
        use_new_centromere=use_new_centromere,
        cache_tasks=cache_tasks,
        mark_as_release=mark_as_release,
        dockerfile_path=dockerfile,
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
    """[DEPRECATED] Launch a workflow using a python parameter map.

    This command requires parameters in an obscure pre-serialized format which
    is not properly documented or easy to create by hand.

    To programmatically launch a workflow, use the SDK API:
    https://wiki.latch.bio/workflows/sdk/testing-and-debugging-a-workflow/programmatic-execution

    CLI relaunch may be reimplemented in the future, so please let us know if this
    would be useful for you.
    """

    click.secho(
        "`latch launch` is deprecated. See `latch launch --help` for details and alternatives.",
        fg="yellow",
    )

    from latch_cli.services.launch.launch import launch

    try:
        wf_name = launch(params_file, version)
    except Exception as e:
        traceback.print_exc()
        raise click.exceptions.Exit(1) from e

    if version is None:
        version = "latest"

    click.secho(
        f"Successfully launched workflow named {wf_name} with version {version}.",
        fg="green",
    )


@main.command("get-params")
@click.argument("wf_name", nargs=1)
@click.option(
    "--version", default=None, help="The version of the workflow. Defaults to latest."
)
@requires_login
def get_params(wf_name: Union[str, None], version: Union[str, None] = None):
    """[DEPRECATED] Generate a python parameter map for a workflow.

    This command will not work properly with workflows using complicated types
    because it is not able to use Python typing information.

    To programmatically launch a workflow, use the SDK API:
    https://wiki.latch.bio/workflows/sdk/testing-and-debugging-a-workflow/programmatic-execution

    CLI relaunch may be reimplemented in the future, so please let us know if this
    would be useful for you.
    """
    click.secho(
        "`latch get-params` is deprecated and frequently broken. See `latch launch --help` for details and alternatives.",
        fg="yellow",
    )

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


@main.command("preview")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True, path_type=Path))
@requires_login
def preview(pkg_root: Path):
    """Creates a preview of your workflow interface."""
    crash_handler.message = f"Unable to preview inputs for {pkg_root}"
    crash_handler.pkg_root = str(pkg_root)

    from latch_cli.services.preview import preview

    preview(pkg_root)


@main.command("get-executions")
@requires_login
def get_executions():
    """Spawns an interactive terminal UI that shows all executions in a given workspace"""

    crash_handler.message = "Unable to fetch executions"

    from latch_cli.services.get_executions import get_executions

    get_executions()


"""
LDATA COMMANDS
"""


@main.command("cp")
@click.argument("src", shell_complete=cp_complete, nargs=-1)
@click.argument("dest", shell_complete=cp_complete, nargs=1)
@click.option(
    "--progress",
    help="Type of progress information to show while copying",
    type=EnumChoice(_Progress, case_sensitive=False),
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
@click.option(
    "--cores", help="Manually specify number of cores to parallelize over", type=int
)
@click.option(
    "--chunk-size-mib",
    help="Manually specify the upload chunk size in MiB. Must be >= 5",
    type=int,
)
@requires_login
def cp(
    src: list[str],
    dest: str,
    progress: _Progress,
    verbose: bool,
    no_glob: bool,
    cores: Optional[int] = None,
    chunk_size_mib: Optional[int] = None,
):
    """Copy files between Latch Data and local, or between two Latch Data locations.

    Behaves like `cp -R` in Unix. Directories are copied recursively. If any parents of dest do not exist, the copy will fail.
    """
    crash_handler.message = f"Unable to copy {src} to {dest}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.cp.main import cp

    cp(
        src,
        dest,
        progress=progress,
        verbose=verbose,
        expand_globs=not no_glob,
        cores=cores,
        chunk_size_mib=chunk_size_mib,
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
def ls(paths: tuple[str], group_directories_first: bool):
    """List the contents of a Latch Data directory"""

    crash_handler.message = f"Unable to display contents of {paths}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.ls import ls

    # If the user doesn't provide any arguments, default to root
    if len(paths) == 0:
        paths = ("/",)

    for path in paths:
        if len(paths) > 1:
            click.echo(f"{path}:")

        ls(path, group_directories_first=group_directories_first)

        if len(paths) > 1:
            click.echo("")


@main.command("rmr")
@click.argument("remote_path", nargs=1, type=str)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    default=False,
    type=bool,
    help="Skip the confirmation dialog.",
)
@click.option(
    "--no-glob",
    "-G",
    help="Don't expand globs in remote paths",
    is_flag=True,
    default=False,
    show_default=True,
)
@click.option(
    "--verbose",
    "-v",
    help="Print all files when deleting",
    is_flag=True,
    default=False,
    show_default=True,
)
@requires_login
def rmr(remote_path: str, yes: bool, no_glob: bool, verbose: bool):
    """Deletes a remote entity."""
    crash_handler.message = f"Unable to delete {remote_path}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.rm import rmr

    rmr(remote_path, skip_confirmation=yes, no_glob=no_glob, verbose=verbose)


@main.command("mkdirp")
@click.argument("remote_directory", nargs=1, type=str)
@requires_login
def mkdir(remote_directory: str):
    """Creates a new remote directory."""
    crash_handler.message = f"Unable to create directory {remote_directory}"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.mkdir import mkdirp

    mkdirp(remote_directory)


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
@click.option("--cores", help="Number of cores to use for parallel syncing.", type=int)
@requires_login
def sync(
    srcs: list[str],
    dst: str,
    delete: bool,
    ignore_unsyncable: bool,
    cores: Optional[int] = None,
):
    """Update the contents of a remote directory with local data."""
    from latch_cli.services.sync import sync

    # todo(maximsmol): remote -> local
    # todo(maximsmol): remote -> remote
    sync(srcs, dst, delete=delete, ignore_unsyncable=ignore_unsyncable, cores=cores)


"""
NEXTFLOW COMMANDS
"""


@main.group()
def nextflow():
    """Manage nextflow"""


@nextflow.command("version")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True, path_type=Path))
def version(pkg_root: Path):
    """Get the Latch version of Nextflow installed for the current project."""

    version = (pkg_root / ".latch" / "nextflow_version").read_text().strip()

    click.secho(f"Nextflow version: {version}", fg="green")


@nextflow.command("generate-entrypoint")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--metadata-root",
    type=click.Path(exists=False, path_type=Path, file_okay=False),
    help="Path to directory containing Latch metadata.",
)
@click.option(
    "--nf-script",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to the nextflow entrypoint to register.",
)
@click.option(
    "--execution-profile",
    type=str,
    default=None,
    help="Set execution profile for Nextflow workflow",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Where to write the entrypoint file. Defaults to wf/custom_entrypoint.py. If the filename "
        "does not end with a .py suffix, one will be appended."
    ),
)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation dialog.")
def generate_entrypoint(
    pkg_root: Path,
    metadata_root: Optional[Path],
    nf_script: Path,
    execution_profile: Optional[str],
    output: Optional[Path],
    yes: bool,
):
    """Generate a `wf/entrypoint.py` file from a Nextflow workflow"""

    from latch.types import metadata
    from latch_cli.nextflow.workflow import generate_nextflow_workflow
    from latch_cli.services.register.utils import import_module_by_path

    if output is None:
        output = pkg_root / "wf" / "custom_entrypoint.py"

    output = output.with_suffix(".py")

    if not yes and not click.confirm(
        f"Will generate an entrypoint at {output}. Proceed?"
    ):
        raise click.exceptions.Abort

    output.parent.mkdir(exist_ok=True)

    if (
        not yes
        and output.exists()
        and not click.confirm(
            f"Nextflow entrypoint already exists at `{output}`. Overwrite?"
        )
    ):
        return

    if metadata_root is None:
        metadata_root = pkg_root / "latch_metadata"

    meta = metadata_root / "__init__.py"
    if meta.exists():
        click.echo(f"Using metadata file {click.style(meta, italic=True)}")
        import_module_by_path(meta)

    if metadata._nextflow_metadata is None:
        click.secho(
            dedent(f"""\
            Failed to generate Nextflow entrypoint.
            Make sure the project root contains a `{meta}`
            with a `NextflowMetadata` object defined.
            """),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    generate_nextflow_workflow(
        pkg_root, metadata_root, nf_script, output, execution_profile=execution_profile
    )


@nextflow.command("attach")
@click.option(
    "--execution-id", "-e", type=str, help="Optional execution ID to inspect."
)
@requires_login
def attach(execution_id: Optional[str]):
    """Drops the user into an interactive shell to inspect the workdir of a nextflow execution."""

    from latch_cli.services.k8s.attach import attach

    attach(execution_id)


@nextflow.command("register")
@click.argument(
    "pkg_root", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation dialogs.")
@click.option(
    "--no-ignore",
    "--all",
    "-a",
    is_flag=True,
    help="Add all files (including those excluded by .gitignore/.dockerignore) to workflow archive",
)
@click.option(
    "--disable-auto-version",
    "-d",
    is_flag=True,
    help="Only use the contents of the version file for workflow versioning.",
)
@click.option(
    "--disable-git-version",
    "-G",
    is_flag=True,
    help="When the package root is a git repository, do not append the current commit hash to the version.",
)
@click.option(
    "--script-path",
    type=str,
    default="main.nf",
    help="Path to the entrypoint nextflow file. Must be relative to the package root.",
)
@requires_login
def nf_register(
    pkg_root: Path,
    yes: bool,
    no_ignore: bool,
    disable_auto_version: bool,
    disable_git_version: bool,
    script_path: str,
):
    from latch_cli.nextflow.forch_register import RegisterConfig, register

    if not (pkg_root / script_path).exists():
        click.secho(
            f"""\
            Could not locate entrypoint file '{script_path}'. Ensure that the path exists, or provide
            another using `--script-path`
            """.strip(),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    # >>> Version parsing

    version_file = pkg_root / "version"
    try:
        version_base = version_file.read_text().strip()
    except OSError:
        if not yes and not click.confirm(
            "Could not find a `version` file in the package root. One will be created. Proceed?"
        ):
            return

        version_base = "0.1.0"
        version_file.write_text(version_base)
        click.echo(f"Created a version file with initial version {version_base}.")

    components: list[str] = [version_base]

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

    # >>> Display Name parsing

    dotfile_root = pkg_root / ".latch"
    dotfile_root.mkdir(exist_ok=True)

    workflow_name_file = dotfile_root / "workflow_name"
    try:
        workflow_name = workflow_name_file.read_text()
    except OSError:
        workflow_name = click.prompt("What is the name of this workflow?")

        if not yes and not click.confirm(
            "This workflow name will be stored in a file at `.latch/workflow_name` under the package root for future use. Proceed?"
        ):
            return

        workflow_name_file.write_text(workflow_name)
        click.echo("Stored workflow name in .latch/workflow_name.")
        click.echo()

    assert isinstance(workflow_name, str)

    click.echo(
        dedent(f"""\
        {click.style("Workflow Name", fg="bright_blue")}: {workflow_name}
        {click.style("Version", fg="bright_blue")}: {version}
        {click.style("Workspace", fg="bright_blue")}: {current_workspace()}
        """).strip()
    )

    click.echo()

    res = gql_execute(
        gql.gql("""
            query WorkflowVersionExistenceCheck(
                $argWorkspaceId: BigInt!
                $argWorkflowName: String!
                $argWorkflowVersion: String!
            ) {
                workflowInfos(
                    condition: {
                        name: $argWorkflowName
                        version: $argWorkflowVersion
                        ownerId: $argWorkspaceId
                    }
                ) {
                    totalCount
                }
            }
        """),
        {
            "argWorkspaceId": current_workspace(),
            "argWorkflowName": workflow_name,
            "argWorkflowVersion": version,
        },
    )["workflowInfos"]

    if int(res["totalCount"]) > 0:
        click.secho(
            dedent(f"""
            Workflow {workflow_name}:{version} already exists in this workspace.

            Please update either the `.latch/workflow_name` or `version` file(s) and re-register.
            """).strip(),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if not yes and not click.confirm("Start registration?"):
        click.secho("Cancelled", bold=True)
        return

    click.echo()

    register(
        pkg_root,
        config=RegisterConfig(workflow_name, version, Path(script_path), not no_ignore),
    )


"""
POD COMMANDS
"""


@main.group()
def pods():
    """Manage pods"""


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


"""
TEST DATA COMMANDS
"""


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

    crash_handler.message = "Unable to list objects within managed bucket"
    crash_handler.pkg_root = str(Path.cwd())

    from latch_cli.services.test_data.ls import ls

    objects = ls()
    click.secho("Listing your managed objects by full S3 path.\n", fg="green")
    for o in objects:
        print(f"\ts3://latch-public/{o}")
