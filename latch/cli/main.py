"""Entrypoints to service functions through a CLI."""

from pathlib import Path
from typing import List, Union

import click
from latch.services import cp as _cp
from latch.services import execute as _execute
from latch.services import get_wf as _get_wf
from latch.services import init as _init
from latch.services import login as _login
from latch.services import ls as _ls
from latch.services import register as _register


@click.group("latch")
def main():
    """A command line toolchain to register workflows and upload data to Latch.

    Visit docs.latch.bio to learn more.
    """
    ...


@click.command("register")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True))
@click.option(
    "--dockerfile",
    default=None,
    help="An explicit Dockerfile to define your workflow's execution environment",
)
@click.option(
    "--pkg_name",
    default=None,
    help="The name of your workflow package (the folder with __init__.py). This is a mandatory option if --dockerfile is provided",
)
def register(pkg_root: str, dockerfile: Union[str, None], pkg_name: Union[str, None]):
    """Register local workflow code to Latch.

    Visit docs.latch.bio to learn more.
    """
    _register(pkg_root, dockerfile, pkg_name)
    click.secho(
        "Successfully registered workflow. View @ console.latch.bio.", fg="green"
    )


@click.command("login")
def login():
    """Manually login to Latch.

    Visit docs.latch.bio to learn more.
    """
    _login()
    click.secho("Successfully logged into LatchBio.", fg="green")


@click.command("init")
@click.argument("pkg_name", nargs=1)
def init(pkg_name: str):
    """Initialize boilerplate for local workflow code.

    Visit docs.latch.bio to learn more.
    """
    _init(pkg_name)
    click.secho(f"Created a latch workflow called {pkg_name}.", fg="green")
    click.secho("Run", fg="green")
    click.secho(f"\t$ latch register {pkg_name}", fg="green")
    click.secho("To register the workflow with console.latch.bio.", fg="green")


@click.command("cp")
@click.argument("source_file", nargs=1)
@click.argument("destination_file", nargs=1)
def cp(source_file: str, destination_file: str):
    """Copy local files to LatchData and vice versa.

    Visit docs.latch.bio to learn more.
    """
    _cp(source_file, destination_file)
    click.secho(
        f"Successfully copied {source_file} to {destination_file}.", fg="green")


@click.command("ls")
# Allows the user to provide unlimited arguments (including zero)
@click.argument("remote_directories", nargs=-1)
def ls(remote_directories: Union[None, List[str]]):
    """List remote files in the command line. Supports multiple directory arguments.

    Visit docs.latch.bio to learn more.
    """
    _item_padding = 3

    # If the user doesn't provide any arguments, default to root
    if not remote_directories:
        remote_directories = ["latch:///"]

    # conditional formatting based on whether the user asks for multiple ls's or not
    _initial_padding = 0 if len(remote_directories) < 2 else 3

    def _emit_directory_header(remote_directory):
        if len(remote_directories) > 1:
            click.secho(f"{remote_directory}:")
            click.secho("")

    def _emit_directory_footer():
        if len(remote_directories) > 1:
            click.secho("")

    for remote_directory in remote_directories:
        output, max_lengths = _ls(remote_directory, padding=_item_padding)

        header_name_padding = " " * (max_lengths["name"] - len("Name"))
        header_content_type_padding = " " * \
            (max_lengths["content_type"] - len("Type"))
        header_content_size_padding = " " * \
            (max_lengths["content_size"] - len("Size"))
        header_modify_time_padding = " " * \
            (max_lengths["modify_time"] - len("Last Modified"))

        header = f"Name{header_name_padding}Type{header_content_type_padding}Size{header_content_size_padding}Last Modified{header_modify_time_padding}"

        _emit_directory_header(remote_directory=remote_directory)

        click.secho(" " * _initial_padding, nl=False)
        click.secho(header, underline=True)

        for row in output:
            name, t, content_type, content_size, modify_time = row

            style = {
                "fg": "cyan" if t == "obj" else "green",
                "bold": True,
            }

            name_padding = max_lengths["name"] - len(name)
            content_type_padding = max_lengths["content_type"] - \
                len(content_type)
            content_size_padding = max_lengths["content_size"] - \
                len(content_size)

            output_str = f"{name}{name_padding}{content_type}{content_type_padding}{content_size}{content_size_padding}{modify_time}"

            click.secho(" " * _initial_padding, nl=False)
            click.secho(output_str, **style)

        _emit_directory_footer()


@click.command("execute")
@click.argument("params_file", nargs=1, type=click.Path(exists=True))
@click.option(
    "--version",
    default=None,
    help="The version of the workflow to execute. Defaults to latest.",
)
def execute(params_file: Path, version: Union[str, None] = None):
    """Execute a workflow using a python parameter map.

    Visit docs.latch.bio to learn more.
    """
    wf_name = _execute(params_file, version)
    if version is None:
        version = "latest"
    click.secho(
        f"Successfully launched workflow named {wf_name} with version {version}.",
        fg="green",
    )


@click.command("get-wf")
@click.option(
    "--name",
    default=None,
    help="The name of the workflow to list. Will display all versions",
)
def get_wf(name: Union[str, None] = None):
    """List workflows.

    Visit docs.latch.bio to learn more.
    """
    wfs = _get_wf(name)
    id_padding, name_padding, version_padding = 0, 0, 0
    for wf in wfs:
        id, name, version = wf
        id_len, name_len, version_len = len(str(id)), len(name), len(version)
        id_padding = max(id_padding, id_len)
        name_padding = max(name_padding, name_len)
        version_padding = max(version_padding, version_len)

    click.secho(
        f"ID{id_padding * ' '}\tName{name_padding * ' '}\tVersion{version_padding * ' '}")
    for wf in wfs:
        click.secho(
            f"{wf[0]}{(id_padding - len(str(wf[0]))) * ' '}\t{wf[1]}{(name_padding - len(wf[1])) * ' '}\t{wf[2]}{(version_padding - len(wf[2])) * ' '}")


main.add_command(register)
main.add_command(login)
main.add_command(init)
main.add_command(cp)
main.add_command(ls)
main.add_command(execute)
main.add_command(get_wf)
