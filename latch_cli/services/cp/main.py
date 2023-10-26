from pathlib import Path
from textwrap import dedent
from typing import List

import click

from latch_cli.services.cp.config import CPConfig, Progress
from latch_cli.services.cp.download import download
from latch_cli.services.cp.glob import expand_pattern
from latch_cli.services.cp.remote_copy import remote_copy
from latch_cli.services.cp.upload import upload
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

    config = CPConfig(
        progress=progress,
        verbose=verbose,
    )

    for src in srcs:
        src_remote = is_remote_path(src)

        if src_remote and not dest_remote:
            if expand_globs:
                [download(p, Path(dest), config) for p in expand_pattern(src)]
            else:
                download(src, Path(dest), config)
        elif not src_remote and dest_remote:
            upload(src, dest, config)
        elif src_remote and dest_remote:
            if expand_globs:
                [remote_copy(p, dest) for p in expand_pattern(src)]
            else:
                remote_copy(src, dest)
        else:
            click.secho(
                dedent(f"""
                `latch cp` cannot be used for purely local file copying.

                Please ensure at least one of your arguments is a remote path (beginning with `latch://`)
                """).strip("\n"),
                fg="red",
            )
            raise click.exceptions.Exit(1)
