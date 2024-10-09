import asyncio
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, List

import aiohttp
import tqdm
import uvloop

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


async def work_loop(
    work_queue: asyncio.Queue[Work],
    tbar: tqdm.tqdm,
    show_task_progress: bool,
    print_file_on_completion: bool,
) -> int:
    pbar = tqdm.tqdm(
        total=0,
        leave=False,
        unit="B",
        unit_scale=True,
        disable=not show_task_progress,
    )

    total_bytes = 0

    async with RetryClientSession(read_timeout=90, conn_timeout=10) as sess:
        while True:
            try:
                work = work_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            try:
                if work.dest.exists() and work.dest.is_dir():
                    shutil.rmtree(work.dest)

                async with sess.get(work.url) as res:
                    total_size = res.content_length
                    assert total_size is not None

                total_bytes += total_size

                pbar.total = total_size
                pbar.desc = work.dest.name

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
                    pbar.write(work.dest.name)

            except Exception as e:
                raise Exception(f"{work}: {e}")

            pbar.reset()
            tbar.update(1)

    pbar.clear()
    return total_bytes


def worker(
    work_queue: asyncio.Queue[Work],
    tbar: tqdm.tqdm,
    show_task_progress: bool,
    print_file_on_completion: bool,
) -> int:
    uvloop.install()

    loop = uvloop.new_event_loop()
    return loop.run_until_complete(
        work_loop(work_queue, tbar, show_task_progress, print_file_on_completion)
    )
