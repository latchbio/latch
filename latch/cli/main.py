"""Entrypoints to service functions through a CLI."""

from typing import Union

import click
from latch.services import cp as _cp
from latch.services import init as _init
from latch.services import login as _login
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
@click.argument("file_1", nargs=1)
@click.argument("file_2", nargs=1)
def cp(file_1: str, file_2: str):
    """Copy local files to LatchData.

    Visit docs.latch.bio to learn more.
    """
    _cp(file_1, file_2)
    click.secho(f"Successfully copied {file_1} to {file_2}.", fg="green")


main.add_command(register)
main.add_command(login)
main.add_command(init)
main.add_command(cp)
