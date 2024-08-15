import math
import mimetypes
import os
import random
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed, wait
from contextlib import closing
from dataclasses import dataclass
from http.client import HTTPException
from multiprocessing.managers import DictProxy, ListProxy
from pathlib import Path
from queue import Queue
from typing import TYPE_CHECKING, List, Optional, TypedDict

from latch_sdk_config.latch import config as latch_config
from typing_extensions import TypeAlias

from latch.ldata.type import LatchPathError, LDataNodeType
from latch_cli import tinyrequests
from latch_cli.constants import Units, latch_constants
from latch_cli.utils import get_auth_header, urljoins, with_si_suffix
from latch_cli.utils.path import normalize_path

from .manager import TransferStateManager
from .node import get_node_data
from .progress import Progress, ProgressBars
from .throttle import Throttle
from .utils import get_max_workers

if TYPE_CHECKING:
    PathQueueType: TypeAlias = "Queue[Optional[Path]]"
    LatencyQueueType: TypeAlias = "Queue[Optional[float]]"
    PartsBySrcType: TypeAlias = DictProxy[Path, ListProxy["CompletedPart"]]
    UploadInfoBySrcType: TypeAlias = DictProxy[Path, "StartUploadReturnType"]


class StartUploadData(TypedDict):
    upload_id: str
    urls: List[str]


@dataclass(frozen=True)
class UploadJob:
    src: Path
    dest: str


@dataclass(frozen=True)
class UploadResult:
    num_files: int
    total_bytes: int
    total_time: float


def upload(
    src: str,  # pathlib.Path strips trailing slashes but we want to keep them here as they determine cp behavior
    dest: str,
    progress: Progress,
    verbose: bool,
    create_parents: bool = False,
    cores: Optional[int] = None,
    chunk_size_mib: Optional[int] = None,
) -> UploadResult:
    src_path = Path(src)
    if not src_path.exists():
        raise ValueError(f"could not find {src_path}: no such file or directory.")

    normalized = normalize_path(dest)

    node_data = get_node_data(dest, allow_resolve_to_parent=True)
    assert dest in node_data.data
    dest_data = node_data.data[dest]

    if not (dest_data.exists() or dest_data.is_direct_parent()) and not create_parents:
        raise LatchPathError("no such Latch file or directory", dest)

    dest_is_dir = dest_data.type in {
        LDataNodeType.account_root,
        LDataNodeType.mount,
        LDataNodeType.mount_gcp,
        LDataNodeType.mount_azure,
        LDataNodeType.dir,
    }

    if not dest_is_dir:
        if not dest_data.exists():  # path is latch:///a/b/file_1/file_2
            raise ValueError(f"no such file or directory: {dest}")
        if src_path.is_dir():
            raise ValueError(f"{normalized} is not a directory.")

    if dest.endswith("/") and not dest_data.exists():
        # path is latch:///a/b/ but b does not exist yet
        if not create_parents:
            raise ValueError(f"no such file or directory: {dest}")
        normalized = urljoins(normalized, src_path.name)

    if cores is None:
        cores = get_max_workers()

    if progress == Progress.none:
        num_bars = 0
        show_total_progress = False
    elif not src_path.is_dir():
        num_bars = 1
        show_total_progress = False
    else:
        num_bars = cores
        show_total_progress = True

    with ProcessPoolExecutor(max_workers=cores) as exec:
        with TransferStateManager() as man:
            parts_by_src: "PartsBySrcType" = man.dict()
            upload_info_by_src: "UploadInfoBySrcType" = man.dict()

            if src_path.is_dir():
                if dest_data.exists() and not src.endswith("/"):
                    normalized = urljoins(normalized, src_path.name)

                jobs: List[UploadJob] = []
                total_bytes = 0

                throttle: Throttle = man.Throttle()
                latency_q: "LatencyQueueType" = man.Queue()
                throttle_listener = exec.submit(throttler, throttle, latency_q)

                for dir_path, _, file_names in os.walk(src_path, followlinks=True):
                    for file_name in file_names:
                        rel_path = Path(dir_path) / file_name

                        try:
                            total_bytes += rel_path.stat().st_size
                        except FileNotFoundError:
                            print(f"WARNING: file {rel_path} not found, skipping...")
                            continue

                        parts_by_src[rel_path] = man.list()
                        jobs.append(
                            UploadJob(
                                rel_path,
                                urljoins(
                                    normalized,
                                    str(rel_path.relative_to(src_path)),
                                ),
                            )
                        )

                num_files = len(jobs)

                url_generation_bar: ProgressBars
                with closing(
                    man.ProgressBars(
                        0,
                        show_total_progress=(progress != Progress.none),
                    )
                ) as url_generation_bar:
                    url_generation_bar.set_total(num_files, "Generating URLs")

                    start_upload_futs: List[Future[Optional[StartUploadReturnType]]] = (
                        []
                    )

                    start = time.monotonic()
                    for job in jobs:
                        start_upload_futs.append(
                            exec.submit(
                                start_upload,
                                job.src,
                                job.dest,
                                url_generation_bar,
                                throttle,
                                latency_q,
                                chunk_size_mib,
                            )
                        )

                    for fut in as_completed(start_upload_futs):
                        res = fut.result()
                        if res is not None:
                            upload_info_by_src[res.src] = res

                    latency_q.put(None)
                    wait([throttle_listener])

                chunk_upload_bars: ProgressBars
                with closing(
                    man.ProgressBars(
                        min(num_bars, num_files),
                        show_total_progress=show_total_progress,
                        verbose=verbose,
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
                        chunk_upload_bars.set_usage(str(res.src), res.part_count)

                        for part_index, url in enumerate(res.urls):
                            chunk_futs.append(
                                exec.submit(
                                    upload_file_chunk,
                                    src=res.src,
                                    url=url,
                                    part_index=part_index,
                                    part_size=res.part_size,
                                    progress_bars=chunk_upload_bars,
                                    pbar_index=pbar_index,
                                    parts_by_source=parts_by_src,
                                    upload_id=res.upload_id,
                                    dest=res.dest,
                                )
                            )

                    for fut in as_completed(chunk_futs):
                        exc = fut.exception()
                        if exc is not None:
                            raise exc

                if progress != Progress.none:
                    print("\x1b[0GFinalizing uploads...")
            else:
                if dest_data.exists() and dest_is_dir:
                    normalized = urljoins(normalized, src_path.name)

                num_files = 1
                total_bytes = src_path.stat().st_size

                progress_bars: ProgressBars
                with closing(
                    man.ProgressBars(
                        num_bars,
                        show_total_progress=show_total_progress,
                        verbose=verbose,
                    )
                ) as progress_bars:
                    pbar_index = progress_bars.get_free_task_bar_index()

                    start = time.monotonic()
                    res = start_upload(
                        src_path, normalized, chunk_size_mib=chunk_size_mib
                    )

                    if res is not None:
                        progress_bars.set(
                            pbar_index, res.src.stat().st_size, res.src.name
                        )

                        chunk_futs: List[Future[CompletedPart]] = []
                        for part_index, url in enumerate(res.urls):
                            chunk_futs.append(
                                exec.submit(
                                    upload_file_chunk,
                                    src_path,
                                    url,
                                    part_index,
                                    res.part_size,
                                    progress_bars,
                                    pbar_index,
                                )
                            )

                        for fut in as_completed(chunk_futs):
                            exc = fut.exception()
                            if exc is not None:
                                raise exc

                        end_upload(
                            normalized,
                            res.upload_id,
                            [fut.result() for fut in chunk_futs],
                        )

    end = time.monotonic()
    total_time = end - start

    return UploadResult(num_files, total_bytes, total_time)


@dataclass(frozen=True)
class StartUploadReturnType:
    upload_id: str
    urls: List[str]
    part_count: int
    part_size: int
    src: Path
    dest: str


MAX_RETRIES = 5


def start_upload(
    src: Path,
    dest: str,
    progress_bars: Optional[ProgressBars] = None,
    throttle: Optional[Throttle] = None,
    latency_q: Optional["LatencyQueueType"] = None,
    chunk_size_mib: Optional[int] = None,
) -> Optional[StartUploadReturnType]:
    if not src.exists():
        raise ValueError(f"could not find {src}: no such file or link")

    resolved = src
    if src.is_symlink():
        resolved = src.resolve()

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
            f"file is {with_si_suffix(file_size)} which exceeds the maximum"
            " upload size (5TiB)",
        )

    if chunk_size_mib is None:
        chunk_size = latch_constants.file_chunk_size
    else:
        chunk_size = chunk_size_mib * Units.MiB

    part_count = min(
        latch_constants.maximum_upload_parts,
        math.ceil(file_size / chunk_size),
    )
    part_size = max(
        chunk_size,
        math.ceil(file_size / latch_constants.maximum_upload_parts),
    )

    if throttle is not None:
        time.sleep(throttle.get_delay())

    start = time.monotonic()
    res = tinyrequests.post(
        latch_config.api.data.start_upload,
        headers={"Authorization": get_auth_header()},
        json={
            "path": dest,
            "content_type": content_type,
            "part_count": part_count,
        },
        num_retries=MAX_RETRIES,
    )
    end = time.monotonic()

    if latency_q is not None:
        latency_q.put(end - start)

    json_data = res.json()

    if res.status_code != 200:
        raise RuntimeError(f"unable to start upload for {src}: {json_data['error']}")

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
    progress_bars: Optional[ProgressBars] = None,
    pbar_index: Optional[int] = None,
    parts_by_source: Optional["PartsBySrcType"] = None,
    upload_id: Optional[str] = None,
    dest: Optional[str] = None,
) -> CompletedPart:
    # todo(ayush): proper exception handling that aborts everything
    try:
        time.sleep(0.1 * random.random())

        with open(src, "rb") as f:
            f.seek(part_size * part_index)
            data = f.read(part_size)

        res = tinyrequests.put(url, data=data)
        if res.status_code != 200:
            raise HTTPException(
                f"failed to upload part {part_index} of {src}: {res.status_code}"
            )

        etag = res.headers["ETag"]
        assert etag is not None, (
            f"Malformed response from chunk upload for {src}, Part {part_index},"
            f" Headers: {res.headers}"
        )

        ret = CompletedPart(
            src=src,
            etag=etag,
            part_number=part_index + 1,
        )

        if parts_by_source is not None:
            parts_by_source[src].append(ret)

        if progress_bars is not None:
            progress_bars.update(pbar_index, len(data))
            pending_parts = progress_bars.dec_usage(str(src))

            if pending_parts == 0:
                progress_bars.return_task_bar(pbar_index)
                progress_bars.update_total_progress(1)
                progress_bars.write(f"Copied {src}")

                if (
                    dest is not None
                    and parts_by_source is not None
                    and upload_id is not None
                ):
                    end_upload(
                        dest=dest,
                        upload_id=upload_id,
                        parts=list(parts_by_source[src]),
                    )

        return ret
    except:
        if progress_bars is not None:
            progress_bars.return_task_bar(pbar_index)

        raise


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
        err = res.json()["error"]
        if res.status_code == 400:
            raise ValueError(f"upload request invalid: {err}")
        if res.status_code == 401:
            raise RuntimeError(f"authorization failed: {err}")
        raise RuntimeError(
            f"end upload request failed with code {res.status_code}: {err}"
        )

    if progress_bars is not None:
        progress_bars.update_total_progress(1)


def throttler(t: Throttle, q: "LatencyQueueType"):
    ema = 0

    # todo(ayush): these params were tuned via naive grid search uploading a
    # test directory w/ ~19k files, ideally we should do something more rigorous
    alpha = 0.6
    beta = 1 / 60
    threshold = 15  # seconds

    while True:
        latency = q.get()
        if latency is None:
            return

        ema = (1 - alpha) * ema + alpha * latency

        if ema > threshold:
            t.set_delay(beta * ema)
        else:
            t.set_delay(0)
