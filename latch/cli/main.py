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
    _, pkg_name = _register(pkg_root)
    click.secho(f"Successfully registered {pkg_name}.", fg="green")


@click.command("login")
def login():
    _login()


@click.command("init")
@click.argument("pkg_name", nargs=1)
def init(pkg_name: str):
    _init(pkg_name)
    click.secho(f"Created a latch workflow called {pkg_name}.", fg="green")
    click.secho("Run", fg="green")
    click.secho(f"\t$ cd {pkg_name}; latch register . ", fg="green")
    click.secho("To register the workflow with console.latch.bio.", fg="green")


@click.command("cp")
@click.argument("local_file", nargs=1, type=click.Path(exists=True))
@click.argument("remote_dest", nargs=1)
def cp(local_file: str, remote_dest: str):
    _cp(local_file, remote_dest)
    click.secho(f"Copied {local_file} to {remote_dest}.", fg="green")


main.add_command(register)
main.add_command(login)
main.add_command(init)
main.add_command(cp)
