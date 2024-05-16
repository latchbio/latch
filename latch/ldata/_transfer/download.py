import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from itertools import repeat
from pathlib import Path
from typing import Dict, List, Set, TypedDict

import click
from latch_sdk_config.latch import config as latch_config

from latch.ldata.type import LDataNodeType
from latch_cli import tinyrequests
from latch_cli.constants import Units
from latch_cli.utils import get_auth_header, human_readable_time, with_si_suffix
from latch_cli.utils.path import normalize_path

from .manager import TransferStateManager
from .node import get_node_data
from .progress import Progress, ProgressBars, get_free_index
from .utils import get_max_workers


class GetSignedUrlData(TypedDict):
    url: str


class GetSignedUrlsRecursiveData(TypedDict):
    urls: Dict[str, str]


@dataclass(frozen=True, unsafe_hash=True)
class DownloadJob:
    signed_url: str
    dest: Path


@dataclass(frozen=True)
class DownloadResult:
    num_files: int
    total_bytes: int
    total_time: int


def download(
    src: str,
    dest: Path,
    progress: Progress,
    verbose: bool,
    confirm_overwrite: bool = True,
) -> DownloadResult:
    if not dest.parent.exists():
        raise ValueError(
            f"invalid copy destination {dest}. Parent directory {dest.parent} does"
            " not exist."
        )

    normalized = normalize_path(src)
    data = get_node_data(src)
    assert src in data.data
    node_data = data.data[src]

    can_have_children = node_data.type in {
        LDataNodeType.account_root,
        LDataNodeType.dir,
        LDataNodeType.mount,
        LDataNodeType.mount_gcp,
        LDataNodeType.mount_azure,
    }

    if can_have_children:
        endpoint = latch_config.api.data.get_signed_urls_recursive
    else:
        endpoint = latch_config.api.data.get_signed_url

    res = tinyrequests.post(
        endpoint,
        headers={"Authorization": get_auth_header()},
        json={"path": normalized},
    )

    if res.status_code != 200:
        err = res.json()["error"]
        msg = f"failed to fetch presigned url(s) for path {src}"
        if res.status_code == 400:
            raise ValueError(f"{msg}: download request invalid: {err}")
        if res.status_code == 401:
            raise RuntimeError(f"authorization token invalid: {err}")
        raise RuntimeError(f"{msg} with code {res.status_code}: {res.json()['error']}")

    json_data = res.json()
    if can_have_children:
        dir_data: GetSignedUrlsRecursiveData = json_data["data"]

        if dest.exists() and not normalized.endswith("/"):
            dest = dest / node_data.name

        try:
            dest.mkdir(exist_ok=True)
        except FileNotFoundError:
            raise ValueError(f"No such download destination {dest}")
        except (FileExistsError, NotADirectoryError):
            raise ValueError(f"Download destination {dest} is not a directory")

        unconfirmed_jobs: List[DownloadJob] = []
        confirmed_jobs: List[DownloadJob] = []
        rejected_jobs: Set[Path] = set()

        for rel_path, url in dir_data["urls"].items():
            unconfirmed_jobs.append(DownloadJob(url, dest / rel_path))

        for job in unconfirmed_jobs:
            reject_job = False
            for parent in job.dest.parents:
                if parent in rejected_jobs:
                    reject_job = True
                    break

            if reject_job:
                continue

            try:
                job.dest.parent.mkdir(parents=True, exist_ok=True)
                confirmed_jobs.append(job)
            except FileExistsError:
                if confirm_overwrite and click.confirm(
                    f"A file already exists at {job.dest.parent}. Overwrite?",
                    default=False,
                ):
                    job.dest.parent.unlink()
                    job.dest.parent.mkdir(parents=True, exist_ok=True)
                    confirmed_jobs.append(job)
                else:
                    print(f"Skipping {job.dest.parent}, file already exists")
                    rejected_jobs.add(job.dest.parent)

        num_files = len(confirmed_jobs)

        if progress == Progress.none:
            num_bars = 0
            show_total_progress = False
        elif progress == Progress.total:
            num_bars = 0
            show_total_progress = True
        else:
            num_bars = min(get_max_workers(), num_files)
            show_total_progress = True

        with TransferStateManager() as manager:
            progress_bars: ProgressBars
            with closing(
                manager.ProgressBars(
                    num_bars,
                    show_total_progress=show_total_progress,
                    verbose=verbose,
                )
            ) as progress_bars:
                progress_bars.set_total(num_files, "Copying Files")

                start = time.monotonic()

                # todo(ayush): benchmark this against asyncio
                with ProcessPoolExecutor(max_workers=get_max_workers()) as executor:
                    total_bytes = sum(
                        executor.map(
                            download_file,
                            confirmed_jobs,
                            repeat(progress_bars),
                        )
                    )

                end = time.monotonic()
    else:
        file_data: GetSignedUrlData = json_data["data"]

        num_files = 1

        if dest.exists() and dest.is_dir():
            dest = dest / node_data.name

        if progress == Progress.none:
            num_bars = 0
        else:
            num_bars = 1

        with TransferStateManager() as manager:
            progress_bars: ProgressBars
            with closing(
                manager.ProgressBars(
                    num_bars,
                    show_total_progress=False,
                    verbose=verbose,
                )
            ) as progress_bars:
                start = time.monotonic()
                total_bytes = download_file(
                    DownloadJob(file_data["url"], dest),
                    progress_bars,
                )
                end = time.monotonic()

    total_time = end - start

    return DownloadResult(num_files, total_bytes, total_time)


# dest will always be a path which includes the copied file as its leaf
# e.g. download_file("a/b.txt", Path("c/d.txt")) will copy the content of 'b.txt' into 'd.txt'
def download_file(
    job: DownloadJob,
    progress_bars: ProgressBars,
) -> int:
    # todo(ayush): benchmark parallelized downloads using the range header
    with open(job.dest, "wb") as f:
        res = tinyrequests.get(job.signed_url, stream=True)
        if res.status_code != 200:
            raise RuntimeError(
                f"failed to download {job.dest.name}: {res.status_code}:"
                f" {res.json()['error']}"
            )

        total_bytes = res.headers.get("Content-Length")
        assert total_bytes is not None, "Must have a content-length header"

        with get_free_index(progress_bars) as pbar_index:
            progress_bars.set(
                index=pbar_index, total=int(total_bytes), desc=job.dest.name
            )

            start = time.monotonic()
            try:
                for data in res.iter_content(
                    chunk_size=5 * Units.MiB
                ):  # todo(ayush): figure out why chunk_size = None breaks in pods
                    f.write(data)
                    progress_bars.update(pbar_index, len(data))
            finally:
                end = time.monotonic()
                progress_bars.update_total_progress(1)
                progress_bars.write(
                    f"Downloaded {job.dest.name} ({with_si_suffix(int(total_bytes))})"
                    f" in {human_readable_time(end - start)}"
                )

        return int(total_bytes)
