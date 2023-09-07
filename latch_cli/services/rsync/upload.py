from latch_cli.services.rsync.config import RsyncConfig


def upload(
    src: str,  # pathlib.Path strips trailing slashes so this needs to be a string
    dest: str,
    *,
    config: RsyncConfig,
):
    ...
