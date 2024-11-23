from pathlib import Path
from textwrap import dedent
from typing import List, Literal, Optional

import click

from latch_cli.services.cp.glob import expand_pattern
from latch_cli.utils.path import get_path_error, is_remote_path

from .download.main import download
from .upload.main import upload


def _copy_and_print(
    src: str, dst: str, progress: Literal["none", "total", "tasks"]
) -> None:
    from latch.ldata._transfer.remote_copy import remote_copy as _remote_copy

    _remote_copy(src, dst)

    if progress != "none":
        click.echo(dedent(f"""\
            {click.style("Copy Requested.", fg="green")}
            {click.style("Source: ", fg="blue")}{(src)}
            {click.style("Destination: ", fg="blue")}{(dst)}\
        """))


# todo(ayush): come up with a better behavior scheme than unix cp
def cp(
    srcs: List[str],
    dest: str,
    *,
    progress: Literal["none", "total", "tasks"],
    verbose: bool,
    force: bool,
    expand_globs: bool,
    cores: Optional[int] = None,
    chunk_size_mib: Optional[int] = None,
):
    from latch.ldata.type import LatchPathError

    if chunk_size_mib is not None and chunk_size_mib < 5:
        click.secho(
            "The chunk size specified by --chunk-size-mib must be at least 5. You"
            f" provided `{chunk_size_mib}`",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    dest_is_remote = is_remote_path(dest)
    srcs_are_remote = [is_remote_path(src) for src in srcs]

    try:
        if not dest_is_remote and all(srcs_are_remote):
            download(
                srcs,
                Path(dest),
                progress,
                verbose,
                force,
                expand_globs,
                cores,
                chunk_size_mib,
            )
        elif dest_is_remote and not any(srcs_are_remote):
            upload(
                srcs,
                dest,
                progress,
                verbose,
                cores,
                chunk_size_mib,
            )
        elif dest_is_remote and all(srcs_are_remote):
            for src in srcs:
                if expand_globs:
                    [_copy_and_print(p, dest, progress) for p in expand_pattern(src)]
                else:
                    _copy_and_print(src, dest, progress)
        else:
            click.secho(
                dedent(f"""\
                    Invalid arguments. The following argument types are valid:

                    (1) All source arguments are remote paths and destination argument is local (download)
                    (2) All source arguments are local paths and destination argument is remote (upload)
                    (3) All source arguments are remote paths and destination argument is remote (remote copy)\
                """),
                fg="red",
            )
            raise click.exceptions.Exit(1)

    except LatchPathError as e:
        if e.acc_id is not None:
            click.secho(get_path_error(e.remote_path, e.message, e.acc_id), fg="red")

        raise click.exceptions.Exit(1) from e
    except Exception as e:
        click.secho(str(e), fg="red")
        raise click.exceptions.Exit(1) from e
