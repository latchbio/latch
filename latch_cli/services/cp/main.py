from pathlib import Path
from textwrap import dedent
from typing import List

import click

from latch.ldata.path import LPath
from latch.ldata.transfer.progress import Progress
from latch_cli.services.cp.glob import expand_pattern
from latch_cli.utils.path import is_remote_path


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

        try:
            if src_remote and not dest_remote:
                if expand_globs:
                    [
                        LPath(p).download(
                            Path(dest), progress=progress, verbose=verbose
                        )
                        for p in expand_pattern(src)
                    ]
                else:
                    LPath(src).download(Path(dest), progress=progress, verbose=verbose)
            elif not src_remote and dest_remote:
                LPath(dest).upload(src, progress=progress, verbose=verbose)
            elif src_remote and dest_remote:
                if expand_globs:
                    [LPath(p).copy(dest) for p in expand_pattern(src)]
                else:
                    LPath(src).copy(dest)
            else:
                raise ValueError(
                    dedent(f"""
                    `latch cp` cannot be used for purely local file copying.

                    Please ensure at least one of your arguments is a remote path (beginning with `latch://`)
                    """).strip("\n"),
                    fg="red",
                )
        except Exception as e:
            click.secho(str(e), fg="red")
            raise click.exceptions.Exit(1) from e
