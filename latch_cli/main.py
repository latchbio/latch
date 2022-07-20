"""Entrypoints to service functions through a latch_cli."""

import re
from collections import OrderedDict
from pathlib import Path
from typing import List, Union

import click

import latch_cli.click_utils
from latch_cli.click_utils import AnsiCodes as ac
from latch_cli.crash_reporter import CrashReporter

latch_cli.click_utils.patch()


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
    ...


@main.command("register")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True))
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
    "-r",
    "--remote",
    is_flag=True,
    default=False,
    type=bool,
    help="Use a remote server to build workflow.",
)
def register(pkg_root: str, disable_auto_version: bool, remote: bool):
    """Register local workflow code to Latch.

    Visit docs.latch.bio to learn more.
    """
    from latch_cli.services.register import register

    try:
        register(pkg_root, disable_auto_version=disable_auto_version, remote=remote)
        click.secho(
            "Successfully registered workflow. View @ console.latch.bio.", fg="green"
        )
    except Exception as e:
        CrashReporter.report(pkg_path=pkg_root)
        click.secho(f"Unable to register workflow: {str(e)}", fg="red")


@main.command("login")
def login():
    """Manually login to Latch."""
    from latch_cli.services.login import login

    try:
        login()
        click.secho("Successfully logged into LatchBio.", fg="green")
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to log in: {str(e)}", fg="red")


@main.command("init")
@click.argument("pkg_name", nargs=1)
def init(pkg_name: str):
    """Initialize boilerplate for local workflow code."""
    from latch_cli.services.init import init

    # Workflow name must not contain capitals or end in a hyphen or underscore. If it does, we should throw an error
    # Check for capitals
    if any(char.isupper() for char in pkg_name):
        click.secho(
            f"Unable to initialize {pkg_name}: package name must not contain any"
            " upper-case characters",
            fg="red",
        )
        return

    # Check for other illegal characters
    if (
        len(
            re.findall(
                "(?:[a-z0-9]+(?:[._-][a-z0-9]+)*\/)*[a-z0-9]+(?:[._-][a-z0-9]+)*",
                pkg_name,
            )
        )
        != 1
    ):
        click.secho(
            f"Unable to initialize {pkg_name}: package name must match the regular"
            " expression"
            " '(?:[a-z0-9]+(?:[._-][a-z0-9]+)*\/)*[a-z0-9]+(?:[._-][a-z0-9]+)*'",
            fg="red",
        )
        click.secho(
            "This means that the package name must start and end with a lower-case"
            " letter, and may contain hyphens, underscores, and periods",
            fg="red",
        )
        return

    try:
        init(pkg_name)
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to initialize {pkg_name}: {str(e)}", fg="red")
        return
    click.secho(f"Created a latch workflow called {pkg_name}.", fg="green")
    click.secho("Run", fg="green")
    click.secho(f"\t$ latch register {pkg_name}", fg="green")
    click.secho("To register the workflow with console.latch.bio.", fg="green")


@main.command("cp")
@click.argument("source_file", nargs=1)
@click.argument("destination_file", nargs=1)
def cp(source_file: str, destination_file: str):
    """Copy local files to LatchData and vice versa."""
    from latch_cli.services.cp import cp

    try:
        cp(source_file, destination_file)
        click.secho(
            f"\nSuccessfully copied {source_file} to {destination_file}.", fg="green"
        )
    except Exception as e:
        CrashReporter.report()
        click.secho(
            f"Unable to copy {source_file} to {destination_file}: {str(e)}", fg="red"
        )


@main.command("ls")
# Allows the user to provide unlimited arguments (including zero)
@click.option(
    "--group-directories-first",
    "--gdf",
    help="List directories before files.",
    is_flag=True,
    default=False,
)
@click.argument("remote_directories", nargs=-1)
def ls(group_directories_first: bool, remote_directories: Union[None, List[str]]):
    """
    List the contents of a Latch Data directory
    """
    from datetime import datetime

    from latch_cli.services.ls import ls
    from latch_cli.utils import with_si_suffix

    # If the user doesn't provide any arguments, default to root
    if not remote_directories:
        remote_directories = ["latch:///"]

    for remote_directory in remote_directories:
        if len(remote_directories) > 1:
            click.echo(f"{remote_directory}:")

        try:
            output = ls(remote_directory)
        except Exception as e:
            click.secho(
                f"Unable to display contents of {remote_directory}: {str(e)}", fg="red"
            )
            continue
        output.sort(key=lambda row: row["name"])
        if group_directories_first:
            output.sort(key=lambda row: row["type"])

        formatted = []
        for row in output:
            vals = {
                "contentSize": click.style(
                    with_si_suffix(int(row["contentSize"]), suffix="", styled=True),
                    fg="bright_green",
                )
                if row["contentSize"] != "-" and row["type"] != "dir"
                else click.style("-", dim=True),
                "modifyTime": click.style(
                    datetime.fromisoformat(row["modifyTime"]).strftime("%d %b %H:%M"),
                    fg="blue",
                )
                if row["modifyTime"] != "-" and row["type"] != "dir"
                else click.style("-", dim=True),
                "name": row["name"],
            }

            if row["type"] == "dir":
                vals["name"] = (
                    click.style(row["name"], fg="bright_blue", bold=True) + "/"
                )

            formatted.append(vals)

        columns = OrderedDict(
            contentSize="Size", modifyTime="Date Modified", name="Name"
        )

        column_width = {key: len(title) for key, title in columns.items()}
        for row in formatted:
            for key in columns:
                column_width[key] = max(column_width[key], len(click.unstyle(row[key])))

        def pad_styled(x: str, l: int, align_right=False):
            cur = len(click.unstyle(x))

            pad = " " * (l - cur)
            if align_right:
                return pad + x
            return x + pad

        click.echo(
            " ".join(
                pad_styled(
                    click.style(title, underline=True),
                    column_width[key],
                    key == "contentSize",
                )
                for key, title in columns.items()
            )
        )
        for row in formatted:
            click.echo(
                " ".join(
                    pad_styled(row[k], column_width[k], k == "contentSize")
                    for k in columns
                )
            )


@main.command(
    "local-execute",
    help=f"""Execute a workflow within the latest registered container. Run from the
    outside the pkg root, eg. {ac.bold}`latch local-execute myworkflow`{ac.reset} where
    {ac.bold}`myworkflow`{ac.reset} is the directory containing your workflow package.

    This is the same as running:

    \b
        {ac.bold}$ python3 wf/__init__.py{ac.reset}

    Assuming this file contains a snippet conducive to local execution such as:

    \b
        {ac.bold}if __name__ == "__main___":
           my_workflow(a="foo", reads=LatchFile("/users/von/neuman/machine.txt"){ac.reset}

    Visit {ac.underline}https://docs.latch.bio/basics/local_development.html{ac.no_underline} to read more
    about local development.
    """,
)
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True))
def local_execute(pkg_root: Path):
    from latch_cli.services.local_execute import local_execute

    try:
        local_execute(Path(pkg_root).resolve())
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to execute workflow: {str(e)}", fg="red")


@main.command("launch")
@click.argument("params_file", nargs=1, type=click.Path(exists=True))
@click.option(
    "--version",
    default=None,
    help="The version of the workflow to launch. Defaults to latest.",
)
def launch(params_file: Path, version: Union[str, None] = None):
    """Launch a workflow using a python parameter map."""
    from latch_cli.services.launch import launch

    try:
        wf_name = launch(params_file, version)
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to launch workflow: {str(e)}", fg="red")
        return
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
def get_params(wf_name: Union[str, None], version: Union[str, None] = None):
    """Generate a python parameter map for a workflow."""
    from latch_cli.services.get_params import get_params

    try:
        get_params(wf_name, version)
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to generate param map for workflow: {str(e)}", fg="red")
        return
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
def get_wf(name: Union[str, None] = None):
    """List workflows."""
    from latch_cli.services.get import get_wf

    try:
        wfs = get_wf(name)
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to get workflows: {str(e)}", fg="red")
        return
    id_padding, name_padding, version_padding = 0, 0, 0
    for wf in wfs:
        id, name, version = wf
        id_len, name_len, version_len = len(str(id)), len(name), len(version)
        id_padding = max(id_padding, id_len)
        name_padding = max(name_padding, name_len)
        version_padding = max(version_padding, version_len)

    click.secho(
        f"ID{id_padding * ' '}\tName{name_padding * ' '}\tVersion{version_padding * ' '}"
    )
    for wf in wfs:
        click.secho(
            f"{wf[0]}{(id_padding - len(str(wf[0]))) * ' '}\t{wf[1]}{(name_padding - len(wf[1])) * ' '}\t{wf[2]}{(version_padding - len(wf[2])) * ' '}"
        )


@main.command("open")
@click.argument("remote_file", nargs=1, type=str)
def open_remote_file(remote_file: str):
    """Open a remote file in the browser."""
    from latch_cli.services.open_file import open_file

    try:
        open_file(remote_file)
        click.secho(f"Successfully opened {remote_file}.", fg="green")
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to open {remote_file}: {str(e)}", fg="red")


@main.command("rm")
@click.argument("remote_path", nargs=1, type=str)
def rm(remote_path: str):
    """Deletes a remote entity."""
    from latch_cli.services.rm import rm

    try:
        rm(remote_path)
        click.secho(f"Successfully deleted {remote_path}.", fg="green")
    except Exception as e:
        click.secho(f"Unable to delete {remote_path}: {str(e)}", fg="red")


@main.command("mkdir")
@click.argument("remote_directory", nargs=1, type=str)
def mkdir(remote_directory: str):
    """Creates a new remote directory."""
    from latch_cli.services.mkdir import mkdir

    try:
        mkdir(remote_directory)
        click.secho(f"Successfully created directory {remote_directory}.", fg="green")
    except Exception as e:
        CrashReporter.report()
        click.secho(
            f"Unable to create directory {remote_directory}: {str(e)}", fg="red"
        )


@main.command("touch")
@click.argument("remote_file", nargs=1, type=str)
def touch(remote_file: str):
    """Creates an empty text file."""
    from latch_cli.services.touch import touch

    try:
        touch(remote_file)
        click.secho(f"Successfully touched {remote_file}.", fg="green")
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to create {remote_file}: {str(e)}", fg="red")


@main.command("exec")
@click.argument("task_name", nargs=1, type=str)
def execute(task_name: str):
    """Drops the user into an interactive shell from within a task."""
    from latch_cli.services.execute import execute

    try:
        execute(task_name)
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to exec into {task_name}: {str(e)}", fg="red")


@main.command("preview")
@click.argument("workflow_name", nargs=1, type=str)
def preview(workflow_name: str):
    """Creates a preview of your workflow interface."""
    from latch_cli.services.preview import preview

    try:
        preview(workflow_name)
    except Exception as e:
        CrashReporter.report()
        click.secho(f"Unable to preview inputs for {workflow_name}: {str(e)}", fg="red")
