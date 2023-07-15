import math
import mimetypes
import os
import time
from concurrent.futures import Future, ProcessPoolExecutor, wait
from contextlib import closing
from dataclasses import dataclass
from multiprocessing import Queue
from multiprocessing.managers import DictProxy, ListProxy, SyncManager
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, TypedDict

import click
from latch_sdk_config.latch import config as latch_config
from typing_extensions import TypeAlias

from latch_cli import tinyrequests
from latch_cli.constants import latch_constants, units
from latch_cli.services.cp.config import CPConfig, Progress
from latch_cli.services.cp.ldata_utils import LDataNodeType, get_node_data
from latch_cli.services.cp.path_utils import normalize_path
from latch_cli.services.cp.progress import ProgressBarManager, ProgressBars
from latch_cli.services.cp.utils import (
    get_auth_header,
    get_max_workers,
    human_readable_time,
)
from latch_cli.utils import urljoins, with_si_suffix

if TYPE_CHECKING:
    QueueType: TypeAlias = Queue[Optional[Path]]
    PartsBySrcType: TypeAlias = DictProxy[Path, ListProxy["CompletedPart"]]
    UploadInfoBySrcType: TypeAlias = DictProxy[Path, "StartUploadReturnType"]


start_upload_batch_size = 100


class EmptyUploadData(TypedDict):
    version_id: str


class StartUploadData(TypedDict):
    upload_id: str
    urls: List[str]


@dataclass(frozen=True)
class UploadJob:
    src: Path
    dest: str


def upload(
    src: str,  # pathlib.Path strips trailing slashes but we want to keep them here as they determine cp behavior
    dest: str,
    config: CPConfig,
):
    click.clear()

    src_path = Path(src)
    if not src_path.exists():
        raise ValueError(f"Could not find {src_path}.")

    click.secho(f"Uploading {src_path.name}", fg="blue")

    node_data = get_node_data(dest, allow_resolve_to_parent=True)
    dest_data = node_data.data[dest]

    normalized = normalize_path(dest)

    dest_exists = not dest_data.is_parent
    dest_is_dir = dest_data.type in {
        LDataNodeType.account_root,
        LDataNodeType.mount,
        LDataNodeType.dir,
    }

    if not dest_is_dir:
        if not dest_exists:  # path is latch:///a/b/file_1/file_2
            raise ValueError(f"No such file or directory: {dest}")
        if src_path.is_dir():
            raise ValueError(f"{normalized} is not a directory.")

    if config.progress == Progress.none:
        num_bars = 0
        show_total_progress = False
    elif not src_path.is_dir():
        num_bars = 1
        show_total_progress = False
    elif config.progress == Progress.total:
        num_bars = 0
        show_total_progress = True
    else:
        num_bars = get_max_workers()
        show_total_progress = True

    with ProcessPoolExecutor(max_workers=get_max_workers()) as executor:
        with ProcessPoolExecutor(max_workers=get_max_workers()) as end_upload_executor:
            with ProgressBarManager() as pbar_manager:
                with SyncManager() as state_manager:
                    parts_by_src: "PartsBySrcType" = state_manager.dict()
                    upload_info_by_src: "UploadInfoBySrcType" = state_manager.dict()

                    if src_path.is_dir():
                        if dest_exists and not src.endswith("/"):
                            normalized = urljoins(normalized, src_path.name)

                        jobs: List[UploadJob] = []
                        total_bytes = 0

                        for dir_path, _, file_names in os.walk(
                            src_path, followlinks=True
                        ):
                            for file_name in file_names:
                                rel_path = Path(dir_path) / file_name

                                parts_by_src[rel_path] = state_manager.list()
                                jobs.append(
                                    UploadJob(
                                        rel_path,
                                        urljoins(
                                            normalized,
                                            str(rel_path.relative_to(src_path)),
                                        ),
                                    )
                                )

                                total_bytes += rel_path.stat().st_size

                        num_files = len(jobs)

                        url_generation_bar: ProgressBars
                        with closing(
                            pbar_manager.ProgressBars(
                                0,
                                show_total_progress=(config.progress != Progress.none),
                            )
                        ) as url_generation_bar:
                            url_generation_bar.set_total(num_files, "Generating URLs")

                            start_upload_futs: List[
                                Future[Optional[StartUploadReturnType]]
                            ] = []
                            batch_futs: List[
                                Future[Optional[StartUploadReturnType]]
                            ] = []

                            start = time.monotonic()
                            for job in jobs:
                                fut = executor.submit(
                                    start_upload,
                                    job.src,
                                    job.dest,
                                    url_generation_bar,
                                )
                                start_upload_futs.append(fut)
                                batch_futs.append(fut)

                                if len(batch_futs) == start_upload_batch_size:
                                    wait(batch_futs)

                                    for fut in batch_futs:
                                        res = fut.result()
                                        if res is not None:
                                            upload_info_by_src[res.src] = res

                                    batch_futs = []

                            wait(batch_futs)

                        for fut in batch_futs:
                            res = fut.result()
                            if res is not None:
                                upload_info_by_src[res.src] = res

                        queue: "QueueType" = state_manager.Queue()
                        listeners = [
                            end_upload_executor.submit(
                                end_upload_listener,
                                queue=queue,
                                parts_by_src=parts_by_src,
                                upload_info_by_src=upload_info_by_src,
                            )
                            for _ in range(get_max_workers())
                        ]

                        chunk_upload_bars: ProgressBars
                        with closing(
                            pbar_manager.ProgressBars(
                                min(num_bars, num_files),
                                show_total_progress=show_total_progress,
                                verbose=config.verbose,
                            )
                        ) as chunk_upload_bars:
                            chunk_upload_bars.set_total(num_files, "Uploading Files")

                            # todo(ayush): async-ify
                            chunk_futs: List[Future[CompletedPart]] = []
                            for data in start_upload_futs:
                                res = data.result()

                                if res is None:
                                    chunk_upload_bars.update_total_progress(1)
                                    continue

                                pbar_index = chunk_upload_bars.get_free_task_bar_index()
                                chunk_upload_bars.set(
                                    pbar_index,
                                    res.src.stat().st_size,
                                    res.src.name,
                                )
                                chunk_upload_bars.set_usage(
                                    str(res.src), res.part_count
                                )

                                for part_index, url in enumerate(res.urls):
                                    chunk_futs.append(
                                        executor.submit(
                                            upload_file_chunk,
                                            src=res.src,
                                            url=url,
                                            part_index=part_index,
                                            part_size=res.part_size,
                                            progress_bars=chunk_upload_bars,
                                            pbar_index=pbar_index,
                                            queue=queue,
                                            parts_by_source=parts_by_src,
                                        )
                                    )

                            wait(chunk_futs)

                        queue.put(None)
                        print("Finalizing uploads...")
                        wait(listeners)
                    else:
                        if dest_exists and dest_is_dir:
                            normalized = urljoins(normalized, src_path.name)

                        num_files = 1
                        total_bytes = src_path.stat().st_size

                        progress_bars: ProgressBars
                        with closing(
                            pbar_manager.ProgressBars(
                                num_bars,
                                show_total_progress=show_total_progress,
                                verbose=config.verbose,
                            )
                        ) as progress_bars:
                            pbar_index = progress_bars.get_free_task_bar_index()

                            start = time.monotonic()
                            res = start_upload(src_path, normalized)

                            if res is not None:
                                progress_bars.set(
                                    pbar_index, res.src.stat().st_size, res.src.name
                                )

                                chunk_futs: List[Future[CompletedPart]] = []
                                for part_index, url in enumerate(res.urls):
                                    chunk_futs.append(
                                        executor.submit(
                                            upload_file_chunk,
                                            src_path,
                                            url,
                                            part_index,
                                            res.part_size,
                                            progress_bars,
                                            pbar_index,
                                        )
                                    )

                                wait(chunk_futs)

                                end_upload(
                                    normalized,
                                    res.upload_id,
                                    [fut.result() for fut in chunk_futs],
                                )

    end = time.monotonic()
    total_time = end - start

    click.clear()
    click.echo(
        f"""{click.style("Upload Complete", fg="green")}

{click.style("Time Elapsed: ", fg="blue")}{human_readable_time(total_time)}
{click.style("Files Uploaded: ", fg="blue")}{num_files} ({with_si_suffix(total_bytes)})"""
    )


@dataclass(frozen=True)
class StartUploadReturnType:
    upload_id: str
    urls: List[str]
    part_count: int
    part_size: int
    src: Path
    dest: str


def start_upload(
    src: Path,
    dest: str,
    progress_bars: Optional[ProgressBars] = None,
) -> Optional[StartUploadReturnType]:
    if not src.exists():
        raise ValueError(f"Could not find {src}: no such file or link")

    if src.is_symlink():
        src = src.resolve()

    content_type, _ = mimetypes.guess_type(src)
    if content_type is None:
        with open(src, "rb") as f:
            sample = f.read(units.KiB)

        try:
            sample.decode()
            content_type = "text/plain"
        except UnicodeDecodeError:
            content_type = "application/octet-stream"

    file_size = src.stat().st_size
    if file_size > latch_constants.maximum_upload_size:
        raise ValueError(
            f"File is {with_si_suffix(file_size)} which exceeds the maximum upload size"
            " (5TiB)"
        )

    part_count = min(
        latch_constants.maximum_upload_parts,
        math.ceil(file_size / latch_constants.file_chunk_size),
    )
    part_size = max(
        latch_constants.file_chunk_size,
        math.ceil(file_size / latch_constants.maximum_upload_parts),
    )

    res = tinyrequests.post(
        latch_config.api.data.start_upload,
        headers={"Authorization": get_auth_header()},
        json={
            "path": dest,
            "content_type": content_type,
            "part_count": part_count,
        },
    )

    json_data = res.json()

    if res.status_code != 200:
        raise ValueError(json_data["error"])

    if progress_bars is not None:
        progress_bars.update_total_progress(1)

    if "version_id" in json_data["data"]:
        return  # file is empty, so no need to upload any content

    data: StartUploadData = json_data["data"]

    return StartUploadReturnType(
        **data, part_count=part_count, part_size=part_size, src=src, dest=dest
    )


@dataclass(frozen=True)
class CompletedPart:
    src: Path
    etag: str
    part_number: int


def upload_file_chunk(
    src: Path,
    url: str,
    part_index: int,
    part_size: int,
    progress_bars: ProgressBars,
    pbar_index: Optional[int],
    queue: Optional["QueueType"] = None,
    parts_by_source: Optional["PartsBySrcType"] = None,
) -> CompletedPart:
    with open(src, "rb") as f:
        f.seek(part_size * part_index)
        data = f.read(part_size)

    res = tinyrequests.put(url, data=data)
    if res.status_code != 200:
        raise RuntimeError(f"Failed to upload part {part_index} of {src}")

    ret = CompletedPart(
        src=src,
        etag=res.headers["ETag"],
        part_number=part_index + 1,
    )

    if parts_by_source is not None:
        parts_by_source[src].append(ret)

    progress_bars.update(pbar_index, len(data))
    pending_parts = progress_bars.dec_usage(str(src))

    if pending_parts == 0:
        progress_bars.return_task_bar(pbar_index)
        progress_bars.update_total_progress(1)
        progress_bars.write(f"Copied {src}")

        if queue is not None:
            queue.put(src)

    return ret


def end_upload(
    dest: str,
    upload_id: str,
    parts: List[CompletedPart],
    progress_bars: Optional[ProgressBars] = None,
):
    res = tinyrequests.post(
        latch_config.api.data.end_upload,
        headers={"Authorization": get_auth_header()},
        json={
            "path": dest,
            "upload_id": upload_id,
            "parts": [
                {
                    "ETag": part.etag,
                    "PartNumber": part.part_number,
                }
                for part in parts
            ],
        },
    )

    if res.status_code != 200:
        raise RuntimeError(f"Unable to complete file upload: {res.json()['error']}")

    if progress_bars is not None:
        progress_bars.update_total_progress(1)


def end_upload_listener(
    queue: "QueueType",
    parts_by_src: "PartsBySrcType",
    upload_info_by_src: "UploadInfoBySrcType",
):
    while True:
        src = queue.get()
        if src is None:
            queue.put(None)
            return

        upload_info = upload_info_by_src[src]

        end_upload(
            upload_info.dest,
            upload_info.upload_id,
            list(parts_by_src[src]),
        )
