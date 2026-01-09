import asyncio
import os
import random
from asyncio.locks import BoundedSemaphore
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
import tqdm
from aiohttp.client import ClientTimeout

from ....constants import Units
from ..http_utils import RetriesExhaustedException, RetryClientSession

if TYPE_CHECKING:
    from collections.abc import Awaitable


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
    retries = 0

    while retries < 5:
        read_timeout = 5 * 2**retries

        try:
            start = index * chunk_size
            end = start + chunk_size - 1

            res = await sess.get(
                url,
                headers={"Range": f"bytes={start}-{end}"},
                timeout=ClientTimeout(sock_read=read_timeout),
            )
            content = await asyncio.wait_for(res.content.read(), timeout=None)

            pbar.update(os.pwrite(fd, content, start))

            return
        except asyncio.TimeoutError:
            retries += 1
        except aiohttp.ClientPayloadError as e:
            raise Exception(
                f"payload error from {start}-{end} (chunk size {end - start + 1})"
            ) from e

    raise RetriesExhaustedException


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
                pbar.update(0)

                chunk_size = work.chunk_size_mib * Units.MiB

                with work.dest.open("wb") as f:
                    coros: list[Awaitable[None]] = []

                    cur = 0
                    while cur * chunk_size < total_size:
                        coros.append(
                            download_chunk(
                                sess,
                                work.url,
                                f.fileno(),
                                cur,
                                min(chunk_size, total_size - cur * chunk_size),
                                pbar,
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
) -> list[int]:
    return await asyncio.gather(*[
        worker(work_queue, tbar, show_task_progress, print_file_on_completion)
        for _ in range(num_workers)
    ])
