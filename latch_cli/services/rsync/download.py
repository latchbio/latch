import os
from datetime import datetime
from pathlib import Path

import click

from latch_cli.services.rsync.config import RsyncConfig
from latch_cli.utils.ldata import LDataNodeType, NodeData, get_node_data
from latch_cli.utils.path import normalize_path


def _sync_object(
    src: str,
    dest: Path,  # must point to an object
    node_data: NodeData,
    config: RsyncConfig,
):
    if dest.is_dir():
        dest = dest / node_data.name

    sync = False
    if not dest.exists():
        sync = True
        dest.parent.mkdir(exist_ok=True, parents=True)
    elif dest.is_file():
        remote_mt = node_data.modify_time or datetime.now()
        local_mt = datetime.fromtimestamp(os.stat(dest).st_mtime)

        sync = remote_mt > local_mt
    else:
        click.echo(click.style("Unable to sync: ", bold=True, fg="red"), nl=False)
        click.echo(click.style(f"Destination {dest} is not a file.", fg="red"))

        raise click.exceptions.Exit(1)

    if not sync:
        return


def download(
    src: str,
    dest: Path,
    *,
    config: RsyncConfig,
):
    src = normalize_path(src)
    data = get_node_data(src)

    if data.data[src].type == LDataNodeType.obj:
        _sync_object(src, dest, data.data[src], config)
