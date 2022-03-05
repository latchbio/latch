"""Entrypoints to service functions through a CLI."""

from typing import Union

import click
from latch.services import cp as _cp
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
    click.secho(f"Successfully copied {source_file} to {destination_file}.", fg="green")


@click.command("ls")
@click.argument("remote_directory", nargs=1)
def ls(remote_directory: str):
    """Copy local files to LatchData and vice versa.

    Visit docs.latch.bio to learn more.
    """
    _INITIAL_PADDING = 0
    _ITEM_PADDING = 3

    output, max_lengths = _ls(remote_directory, padding=_ITEM_PADDING)

    header_name_padding = max_lengths["name"] - len("Name")
    header_content_type_padding = max_lengths["content_type"] - len("Content Type")
    header_content_size_padding = max_lengths["content_size"] - len("Content Size")
    header_modify_time_padding = max_lengths["modify_time"] - len("Modify Time")

    header = (
        "Name"
        + " " * header_name_padding
        + "Content Type"
        + " " * header_content_type_padding
        + "Content Size"
        + " " * header_content_size_padding
        + "Modify Time"
        + " " * header_modify_time_padding
    )

    click.secho(" " * _INITIAL_PADDING, nl=False)
    click.secho(header, underline=True)

    for row in output:
        name, t, content_type, content_size, modify_time = row

        style = {
            "fg": "cyan" if t == "obj" else "green",
            "bold": True,
        }

        name_padding = max_lengths["name"] - len(name)
        content_type_padding = max_lengths["content_type"] - len(content_type)
        content_size_padding = max_lengths["content_size"] - len(content_size)

        output_str = (
            name
            + " " * name_padding
            + content_type
            + " " * content_type_padding
            + content_size
            + " " * content_size_padding
            + modify_time
        )

        click.secho(" " * _INITIAL_PADDING, nl=False)
        click.secho(output_str, **style)


main.add_command(register)
main.add_command(login)
main.add_command(init)
main.add_command(cp)
main.add_command(ls)
