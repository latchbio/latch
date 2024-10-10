import asyncio
import math
import mimetypes
import os
import queue
import random
from dataclasses import dataclass
from pathlib import Path
from typing import List, TypedDict

import aiohttp
import click
import tqdm
import uvloop

from latch_cli.constants import Units, latch_constants
from latch_cli.utils import get_auth_header, with_si_suffix

from ..http_utils import RateLimitExceeded, RetryClientSession


@dataclass
class Work:
    src: Path
    dest: str
    chunk_size_mib: int = 16


class StartUploadData(TypedDict):
    upload_id: str
    urls: List[str]


@dataclass
class CompletedPart:
    src: Path
    etag: str
    part: int


async def upload_chunk(
    session: aiohttp.ClientSession,
    src: Path,
    url: str,
    index: int,
    part_size: int,
    pbar: tqdm.tqdm,
) -> CompletedPart:
    with open(src, "rb") as f:
        data = os.pread(f.fileno(), part_size, index * part_size)

    res = await session.put(url, data=data)
    if res.status != 200:
        raise RuntimeError(f"failed to upload part {index} of {src}: {res.content}")

    etag = res.headers["ETag"]
    if etag is None:
        raise RuntimeError(
            f"Malformed response from chunk upload for {src}, Part {index},"
            f" Headers: {res.headers}"
        )

    pbar.update(len(data))

    return CompletedPart(src=src, etag=etag, part=index + 1)


min_part_size = 5 * Units.MiB


async def work_loop(
    work_queue: queue.Queue,
    total_pbar: tqdm.tqdm,
    show_task_progress: bool,
    print_file_on_completion: bool,
):
    pbar = tqdm.tqdm(
        total=0,
        leave=False,
        unit="B",
        unit_scale=True,
        disable=not show_task_progress,
    )

    async with RetryClientSession(read_timeout=90, conn_timeout=10) as sess:
        while True:
            try:
                work: Work = work_queue.get_nowait()
            except queue.Empty:
                break

            resolved = work.src
            if work.src.is_symlink():
                resolved = work.src.resolve()

            content_type, _ = mimetypes.guess_type(resolved)
            if content_type is None:
                with open(resolved, "rb") as f:
                    sample = f.read(Units.KiB)

                try:
                    sample.decode()
                    content_type = "text/plain"
                except UnicodeDecodeError:
                    content_type = "application/octet-stream"

            file_size = resolved.stat().st_size
            if file_size > latch_constants.maximum_upload_size:
                raise ValueError(
                    f"{resolved}: file is {with_si_suffix(file_size)} which exceeds the"
                    " maximum upload size (5TiB)",
                )

            chunk_size = work.chunk_size_mib * Units.MiB
            if chunk_size < min_part_size:
                raise RuntimeError(
                    "Unable to complete upload - please check your internet"
                    " connection speed or any firewall settings that may block"
                    " outbound traffic."
                )

            part_count = min(
                latch_constants.maximum_upload_parts,
                math.ceil(file_size / chunk_size),
            )
            part_size = max(
                chunk_size,
                math.ceil(file_size / latch_constants.maximum_upload_parts),
            )

            pbar.desc = resolved.name
            pbar.total = file_size

            # jitter to not dos nuc-data
            await asyncio.sleep(0.1 * random.random())

            resp = await sess.post(
                "https://nucleus.latch.bio/ldata/start-upload",
                headers={"Authorization": get_auth_header()},
                json={
                    "path": work.dest,
                    "content_type": content_type,
                    "part_count": part_count,
                },
            )
            if resp.status == 429:
                raise RateLimitExceeded(
                    "The service is currently under load and could not complete your"
                    " request - please try again later."
                )

            resp.raise_for_status()

            json_data = await resp.json()
            data: StartUploadData = json_data["data"]

            if "version_id" in data:
                total_pbar.update(1)
                # file is empty - nothing to do
                continue

            try:
                parts = await asyncio.gather(*[
                    upload_chunk(sess, resolved, url, index, part_size, pbar)
                    for index, url in enumerate(data["urls"])
                ])
            except TimeoutError:
                work_queue.put(Work(work.src, work.dest, work.chunk_size_mib // 2))
                continue

            # exception handling
            resp = await sess.post(
                "https://nucleus.latch.bio/ldata/end-upload",
                headers={"Authorization": get_auth_header()},
                json={
                    "path": work.dest,
                    "upload_id": data["upload_id"],
                    "parts": [
                        {
                            "ETag": part.etag,
                            "PartNumber": part.part,
                        }
                        for part in parts
                    ],
                },
            )
            if resp.status == 429:
                raise RateLimitExceeded(
                    "The service is currently under load and could not complete your"
                    " request - please try again later."
                )

            resp.raise_for_status()

            if print_file_on_completion:
                pbar.write(work.src.name)

            pbar.reset()
            total_pbar.update(1)

    pbar.clear()


def worker(
    work_queue: queue.Queue,
    total: tqdm.tqdm,
    show_task_progress: bool,
    print_file_on_completion: bool,
):
    uvloop.install()

    loop = uvloop.new_event_loop()
    loop.run_until_complete(
        work_loop(work_queue, total, show_task_progress, print_file_on_completion)
    )