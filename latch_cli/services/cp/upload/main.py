import asyncio
import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import List, Literal, Optional

import click
import tqdm

from ....utils import human_readable_time, urljoins, with_si_suffix
from ....utils.path import normalize_path
from .worker import Work, worker


def upload(
    srcs: List[str],
    dest: str,
    progress: Literal["none", "total", "tasks"],
    verbose: bool,
    cores: Optional[int],
    chunk_size_mib: Optional[int],
):
    from latch.ldata.type import LatchPathError

    if cores is None:
        cores = 4
    if chunk_size_mib is None:
        chunk_size_mib = 16

    start = time.monotonic()

    from latch.ldata.path import _get_node_data
    from latch.ldata.type import LDataNodeType

    dest_data = _get_node_data(dest).data[dest]
    dest_is_dir = dest_data.type in {
        LDataNodeType.account_root,
        LDataNodeType.mount,
        LDataNodeType.mount_gcp,
        LDataNodeType.mount_azure,
        LDataNodeType.dir,
    }

    work_queue = queue.Queue()
    total_bytes = 0
    num_files = 0

    for src in srcs:
        src_path = Path(src)
        if not src_path.exists():
            raise ValueError(f"{src_path}: no such file or directory")

        normalized = normalize_path(dest)

        if not dest_data.exists:
            root = normalized
        elif src_path.is_dir():
            if not dest_is_dir:
                raise LatchPathError("not a directory", dest)
            if src.endswith("/"):
                root = normalized
            else:
                root = urljoins(normalized, src_path.name)
        else:
            if dest_is_dir:
                root = urljoins(normalized, src_path.name)
            else:
                root = normalized

        if not src_path.is_dir():
            num_files += 1
            total_bytes += src_path.resolve().stat().st_size

            work_queue.put(Work(src_path, root, chunk_size_mib))
        else:

            for dir, _, file_names in os.walk(src_path, followlinks=True):
                for f in file_names:
                    rel = Path(dir) / f

                    try:
                        total_bytes += rel.resolve().stat().st_size
                    except FileNotFoundError:
                        print(f"WARNING: file {rel} not found, skipping...")
                        continue

                    num_files += 1

                    remote = urljoins(root, str(rel.relative_to(src_path)))
                    work_queue.put(Work(rel, remote, chunk_size_mib))

    total = tqdm.tqdm(
        total=num_files,
        leave=False,
        smoothing=0,
        colour="green",
        unit="",
        unit_scale=True,
        disable=progress == "none",
    )

    workers = min(cores, num_files)
    with ThreadPoolExecutor(workers) as exec:
        futs = [
            exec.submit(worker, work_queue, total, progress == "tasks", verbose)
            for _ in range(workers)
        ]

    for f in as_completed(futs):
        f.result()

    total.clear()
    total_time = time.monotonic() - start

    if progress != "none":
        click.echo(dedent(f"""\
            {click.style("Upload Complete", fg="green")}
            {click.style("Time Elapsed:", fg="blue")}   {human_readable_time(total_time)}
            {click.style("Files Uploaded:", fg="blue")} {num_files} ({with_si_suffix(total_bytes)})\
        """))
