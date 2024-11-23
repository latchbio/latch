import asyncio
import os
import queue
import shutil
import time
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Awaitable, List

import aiohttp
import tqdm
import uvloop

from latch_cli.services.cp.utils import chunked

from ....constants import Units
from ..http_utils import RetryClientSession


@dataclass
class Work:
    url: str
    dest: Path
    chunk_size_mib: int = 5


async def download_chunk(
    sess: aiohttp.ClientSession,
    url: str,
    fd: int,
    index: int,
    chunk_size: int,
    pbar: tqdm.tqdm,
):
    start = index * chunk_size
    end = start + chunk_size - 1

    res = await sess.get(url, headers={"Range": f"bytes={start}-{end}"})
    content = await res.read()
    pbar.update(os.pwrite(fd, content, start))


async def worker(
    work_queue: asyncio.Queue[Work],
    tbar: tqdm.tqdm,
    show_task_progress: bool,
    print_file_on_completion: bool,
) -> int:
    pbar = tqdm.tqdm(
        total=0,
        leave=False,
        smoothing=0,
        unit="B",
        unit_scale=True,
        disable=not show_task_progress,
    )
    total_bytes = 0

    try:
        async with RetryClientSession(read_timeout=90, conn_timeout=10) as sess:
            while True:
                try:
                    work: Work = work_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                pbar.reset()
                pbar.desc = work.dest.name

                res = await sess.get(work.url, headers={"Range": "bytes=0-0"})

                # s3 throws a REQUESTED_RANGE_NOT_SATISFIABLE if the file is empty
                if res.status == 416:
                    total_size = 0
                else:
                    content_range = res.headers["Content-Range"]
                    total_size = int(content_range.replace("bytes 0-0/", ""))

                assert total_size is not None

                total_bytes += total_size
                pbar.total = total_size

                chunk_size = work.chunk_size_mib * Units.MiB

                with work.dest.open("wb") as f:
                    coros: List[Awaitable] = []

                    cur = 0
                    while cur * chunk_size < total_size:
                        coros.append(
                            download_chunk(
                                sess, work.url, f.fileno(), cur, chunk_size, pbar
                            )
                        )
                        cur += 1

                    await asyncio.gather(*coros)

                if print_file_on_completion:
                    pbar.write(str(work.dest))

                tbar.update(1)

        return total_bytes
    finally:
        pbar.clear()


async def run_workers(
    work_queue: asyncio.Queue[Work],
    num_workers: int,
    tbar: tqdm.tqdm,
    show_task_progress: bool,
    print_file_on_completion: bool,
) -> List[int]:
    return await asyncio.gather(*[
        worker(work_queue, tbar, show_task_progress, print_file_on_completion)
        for _ in range(num_workers)
    ])
