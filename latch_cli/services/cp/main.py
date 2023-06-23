from glob import glob
from pathlib import Path
from typing import List

import click

from latch_cli.services.cp.config import CPConfig, Progress
from latch_cli.services.cp.download import download
from latch_cli.services.cp.glob import expand_pattern
from latch_cli.services.cp.path_utils import is_remote_path
from latch_cli.services.cp.remote_copy import remote_copy
from latch_cli.services.cp.upload import upload


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
            raise ValueError(
                f"`latch cp` cannot be used for purely local file copying. Please make"
                f" sure one or both of your paths is a remote path (beginning with"
                f" `latch://`)"
            )
