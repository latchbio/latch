from pathlib import Path
from textwrap import dedent
from typing import List

import click

from latch.ldata.transfer.download import _download
from latch.ldata.transfer.progress import Progress
from latch.ldata.transfer.remote_copy import _remote_copy
from latch.ldata.transfer.upload import _upload
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
                        _download(p, Path(dest), progress=progress, verbose=verbose)
                        for p in expand_pattern(src)
                    ]
                else:
                    _download(
                        src, Path(dest), show_progress_bar=progress, verbose=verbose
                    )
            elif not src_remote and dest_remote:
                _upload(src, dest, progress=progress, verbose=verbose)
            elif src_remote and dest_remote:
                if expand_globs:
                    [_remote_copy(p, dest, verbose=True) for p in expand_pattern(src)]
                else:
                    _remote_copy(src, dest, verbose=True)
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
