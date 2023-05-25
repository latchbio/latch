import math
import mimetypes
import os
import time
from concurrent.futures import Future, ProcessPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, TypedDict

import click

from latch_cli import tinyrequests
from latch_cli.config.latch import config as latch_config
from latch_cli.constants import latch_constants, units
from latch_cli.services.cp.config import CPConfig, Progress
from latch_cli.services.cp.ldata_utils import LDataNodeType, get_node_data
from latch_cli.services.cp.path_utils import normalize_path, remote_joinpath
from latch_cli.services.cp.progress import ProgressBarManager, ProgressBars
from latch_cli.services.cp.utils import get_auth_header, human_readable_time, pluralize
from latch_cli.utils import with_si_suffix


class EmptyUploadData(TypedDict):
    version_id: str


class StartUploadData(TypedDict):
    upload_id: str
    urls: List[str]


def upload(
    src: str,  # pathlib.Path strips trailing slashes but we want to keep them here as they determine cp behavior
    dest: str,
    config: CPConfig,
):
    src_path = Path(src)
    if not src_path.exists():
        raise ValueError(f"Could not find {src_path}.")

    normalized = normalize_path(dest)
    dest_data = get_node_data(normalized, allow_resolve_to_parent=True)

    dest_exists = not dest_data.is_parent
    dest_is_dir = dest_data.type in {
        LDataNodeType.account_root,
        LDataNodeType.mount,
        LDataNodeType.dir,
    }

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
        num_bars = config.max_concurrent_files
        show_total_progress = True

    with (
        ProcessPoolExecutor() as executor,
        ProgressBarManager() as manager,
    ):
        if src_path.is_dir():
            if not dest_is_dir:
                raise ValueError(f"{normalized} is not a directory.")

            if dest_exists and not src.endswith("/"):
                normalized = remote_joinpath(normalized, src_path.name)

            srcs: List[Path] = []
            dests: List[str] = []
            total_bytes = 0

            for dir_path, _, file_names in os.walk(src_path, followlinks=True):
                for file_name in file_names:
                    rel_path = Path(dir_path) / file_name

                    srcs.append(rel_path)
                    dests.append(
                        remote_joinpath(normalized, str(rel_path.relative_to(src_path)))
                    )
                    total_bytes += rel_path.stat().st_size

            num_files = len(srcs)

            url_generation_bar: ProgressBars = manager.ProgressBars(
                0, show_total_progress=(config.progress != Progress.none)
            )
            url_generation_bar.set_total(num_files, "Generating URLs")

            start_upload_futs: List[Future[Optional[StartUploadReturnType]]] = []

            start = time.perf_counter()
            for s, d in zip(srcs, dests):
                start_upload_futs.append(
                    executor.submit(
                        start_upload, s, d, config.chunk_size, url_generation_bar
                    )
                )

            wait(start_upload_futs)
            url_generation_bar.close()

            chunk_upload_bars: ProgressBars = manager.ProgressBars(
                min(num_bars, num_files),
                show_total_progress=show_total_progress,
                verbose=config.verbose,
            )
            chunk_upload_bars.set_total(num_files)

            # todo(ayush): this is jank and also gives the illusion of synchronous execution
            # since nothing gets interleaved
            #
            # perhaps do each file in its own process and have the chunks be uploaded with asyncio?
            chunk_futs: List[Future[CompletedPart]] = []
            for data in start_upload_futs:
                res = data.result()

                if res is None:
                    chunk_upload_bars.update_total_progress(1)
                    continue

                pbar_index = chunk_upload_bars.get_free_task_bar_index()
                chunk_upload_bars.set(pbar_index, res.src.stat().st_size, res.src.name)
                chunk_upload_bars.set_usage(pbar_index, res.part_count)

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
                        )
                    )

            wait(chunk_futs)
            chunk_upload_bars.close()

            finalize_uploads_bar: ProgressBars = manager.ProgressBars(
                0, show_total_progress=(config.progress != Progress.none)
            )
            finalize_uploads_bar.set_total(num_files, "Finalizing Uploads")

            parts_by_source: Dict[Path, List[CompletedPart]] = {src: [] for src in srcs}
            for part in chunk_futs:
                res = part.result()
                parts_by_source[res.src].append(res)

            end_upload_futs: List[Future[None]] = []
            for data in start_upload_futs:
                res = data.result()
                if res is None:
                    finalize_uploads_bar.update_total_progress(1)
                    continue

                end_upload_futs.append(
                    executor.submit(
                        end_upload,
                        res.dest,
                        res.upload_id,
                        parts_by_source[res.src],
                        finalize_uploads_bar,
                    )
                )

            wait(end_upload_futs)
            finalize_uploads_bar.close()

            end = time.perf_counter()
        else:
            num_files = 1
            total_bytes = src_path.stat().st_size

            progress_bars: ProgressBars = manager.ProgressBars(
                num_bars,
                show_total_progress=show_total_progress,
                verbose=config.verbose,
            )
            pbar_index = progress_bars.get_free_task_bar_index()

            start = time.perf_counter()
            res = start_upload(src_path, dest, config.chunk_size)

            if res is not None:
                progress_bars.set(pbar_index, res.src.stat().st_size, res.src.name)

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
                    dest,
                    res.upload_id,
                    [fut.result() for fut in chunk_futs],
                )

            end = time.perf_counter()
            progress_bars.close()

    total_time = end - start

    click.secho(
        (
            "Uploaded"
            f" {num_files} {pluralize('file', 'files', num_files)} ({with_si_suffix(total_bytes)})"
            f" in {human_readable_time(total_time)}."
        ),
        fg="green",
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
    src: Path, dest: str, chunk_size: int, progress_bars: Optional[ProgressBars] = None
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
        math.ceil(file_size / chunk_size),
    )
    part_size = max(
        chunk_size,
        math.ceil(file_size / latch_constants.maximum_upload_parts),
    )

    res = tinyrequests.post(
        latch_config.api.data.start_upload,
        headers=get_auth_header(),
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
    pbar_index: int,
) -> CompletedPart:
    with open(src, "rb") as f:
        f.seek(part_size * part_index)
        data = f.read(part_size)

    res = tinyrequests.put(url, data=data)
    if res.status_code != 200:
        raise RuntimeError(f"Failed to upload part {part_index} of {src}")

    progress_bars.update(pbar_index, len(data))
    progress_bars.dec_usage(pbar_index, f"Copied {src.name}")

    return CompletedPart(
        src=src,
        etag=res.headers["ETag"],
        part_number=part_index + 1,
    )


def end_upload(
    dest: str,
    upload_id: str,
    parts: List[CompletedPart],
    progress_bars: Optional[ProgressBars] = None,
):
    res = tinyrequests.post(
        latch_config.api.data.end_upload,
        headers=get_auth_header(),
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
