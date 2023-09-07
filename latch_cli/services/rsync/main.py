# todo(ayush): come up with a better behavior scheme than unix cp
from pathlib import Path
from textwrap import dedent

import click

from latch_cli.services.rsync.config import RsyncConfig
from latch_cli.services.rsync.download import download
from latch_cli.services.rsync.upload import upload
from latch_cli.utils.path import is_remote_path


def cp(
    src: str,
    dest: str,
    *,
    verbose: bool,
):
    dest_remote = is_remote_path(dest)
    src_remote = is_remote_path(src)

    config = RsyncConfig(
        verbose=verbose,
    )

    if src_remote and not dest_remote:
        download(src, Path(dest), config=config)
    elif not src_remote and dest_remote:
        upload(src, dest, config=config)
    elif src_remote and dest_remote:
        # todo(ayush): remote -> remote rsync
        click.secho(
            "`latch rsync` cannot currently be used for remote -> remote file sync.",
            fg="yellow",
        )
    else:
        click.secho(
            dedent("""
            `latch rsync` cannot be used for local -> local file sync.

            Please make sure one of your paths is a remote path (beginning with `latch://`)
            """).strip("\n"),
            fg="red",
        )
