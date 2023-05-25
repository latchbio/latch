from pathlib import Path

from latch_cli.services.cp.config import CPConfig, Progress
from latch_cli.services.cp.download import download
from latch_cli.services.cp.upload import upload
from latch_cli.services.cp.utils import is_remote_path


def cp(
    src: str,
    dest: str,
    *,
    max_concurrent_files: int,
    progress: Progress,
    verbose: bool,
    chunk_size: int,
):
    src_remote = is_remote_path(src)
    dest_remote = is_remote_path(dest)

    config = CPConfig(
        max_concurrent_files=max_concurrent_files,
        progress=progress,
        verbose=verbose,
        chunk_size=chunk_size,
    )

    if src_remote and not dest_remote:
        download(src, Path(dest), config)
    elif not src_remote and dest_remote:
        upload(src, dest, config)
    elif src_remote and dest_remote:
        # todo(ayush): remote copy
        raise NotImplementedError()
    else:
        raise ValueError(f"Neither {src} nor {dest} are remote paths.")
