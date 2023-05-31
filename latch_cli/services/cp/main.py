from pathlib import Path

from latch_cli.services.cp.config import CPConfig, Progress
from latch_cli.services.cp.download import download
from latch_cli.services.cp.path_utils import is_remote_path
from latch_cli.services.cp.remote_copy import remote_copy
from latch_cli.services.cp.upload import upload


# todo(ayush): come up with a better behavior scheme than unix cp
def cp(
    src: str,
    dest: str,
    *,
    progress: Progress,
    verbose: bool,
):
    src_remote = is_remote_path(src)
    dest_remote = is_remote_path(dest)

    config = CPConfig(
        progress=progress,
        verbose=verbose,
    )

    if src_remote and not dest_remote:
        download(src, Path(dest), config)
    elif not src_remote and dest_remote:
        upload(src, dest, config)
    elif src_remote and dest_remote:
        remote_copy(src, dest)
    else:
        raise ValueError(
            f"`latch cp` cannot be used for purely local file copying. Please make sure"
            f" one or both of your paths is a remote path (beginning with `latch://`)"
        )
