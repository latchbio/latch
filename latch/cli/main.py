"""
cli.main
~~~~~~~~~~~~~~
CLI entrypoints.
"""

import click
from latch.services import cp as _cp
from latch.services import init as _init
from latch.services import login as _login
from latch.services import register as _register


@click.group("latch")
def main():
    """A commmand line toolkit to interact with the LatchBio platform"""
    ...


@click.command("register")
@click.argument("pkg_root", nargs=1, type=click.Path(exists=True))
def register(pkg_root: str):

    click.secho(pkg_root)
    click.secho(click.format_filename(pkg_root))

    _register(pkg_root)


@click.command("login")
def login():
    _login()


@click.command("init")
@click.argument("pkg_name", nargs=1)
def init(pkg_name: str):
    _init(pkg_name)


@click.command("cp")
def cp():
    _cp()


main.add_command(register)
main.add_command(login)
main.add_command(init)
main.add_command(cp)
