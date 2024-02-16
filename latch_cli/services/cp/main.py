from pathlib import Path
from textwrap import dedent
from typing import List

import click

from latch.ldata.path import LPath
from latch.ldata.transfer.progress import Progress
from latch_cli.services.cp.glob import expand_pattern
from latch_cli.utils.path import is_remote_path


# todo(ayush): figure out how to do progress for this
def remote_copy(
    src: LPath,
    dest: LPath,
):
    click.clear()

    try:
        src.copy(dest)
    except Exception as e:
        click.echo(str(e))
        raise click.exceptions.Exit(1) from e

    click.echo(f"""
{click.style("Copy Requested.", fg="green")}

{click.style("Source: ", fg="blue")}{(src)}
{click.style("Destination: ", fg="blue")}{(dest)}""")


def upload(src: str, dest: LPath, progress: Progress, verbose: bool):
    try:
        dest.upload(src, progress=progress, verbose=verbose)
    except Exception as e:
        click.echo(str(e))
        raise click.exceptions.Exit(1) from e


def download(src: LPath, dest: str, progress: Progress, verbose: bool):
    try:
        src.download(dest, progress=progress, verbose=verbose)
    except Exception as e:
        click.echo(str(e))
        raise click.exceptions.Exit(1) from e


# todo(ayush): come up with a better behavior scheme than unix cp
def cp(
    srcs: List[str],
    dest: str,
    *,
    progress: Progress,
    verbose: bool,
    expand_globs: bool,
):
    dest_remote = is_remote_path(dest)

    for src in srcs:
        src_remote = is_remote_path(src)

        if src_remote and not dest_remote:
            if expand_globs:
                [
                    download(LPath(p), Path(dest), progress, verbose)
                    for p in expand_pattern(src)
                ]
            else:
                download(LPath(src), Path(dest), progress, verbose)
        elif not src_remote and dest_remote:
            upload(src, LPath(dest), progress, verbose)
        elif src_remote and dest_remote:
            if expand_globs:
                [remote_copy(LPath(p), LPath(dest)) for p in expand_pattern(src)]
            else:
                remote_copy(LPath(src), LPath(dest))
        else:
            click.secho(
                dedent(f"""
                `latch cp` cannot be used for purely local file copying.

                Please ensure at least one of your arguments is a remote path (beginning with `latch://`)
                """).strip("\n"),
                fg="red",
            )
            raise click.exceptions.Exit(1)
