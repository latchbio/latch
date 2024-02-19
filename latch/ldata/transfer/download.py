import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from http.client import HTTPException
from itertools import repeat
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Set, TypedDict

import click
from latch_sdk_config.latch import config as latch_config

from latch.ldata.path import LDataNodeType
from latch_cli import tinyrequests
from latch_cli.constants import Units
from latch_cli.utils import get_auth_header, with_si_suffix
from latch_cli.utils.path import normalize_path

from .manager import _TransferStateManager
from .node import _get_node_data
from .progress import Progress, _get_free_index, _ProgressBars
from .utils import _get_max_workers, _human_readable_time


class _GetSignedUrlData(TypedDict):
    url: str


class _GetSignedUrlsRecursiveData(TypedDict):
    urls: Dict[str, str]


@dataclass(frozen=True, unsafe_hash=True)
class _DownloadJob:
    signed_url: str
    dest: Path


def _download(
    src: str,
    dest: Path,
    progress: Progress,
    verbose: bool,
    confirm_overwrite: bool = True,
) -> None:
    if not dest.parent.exists():
        raise ValueError(
            f"Invalid copy destination {dest}. Parent directory {dest.parent} does not"
            " exist."
        )

    normalized = normalize_path(src)
    data = _get_node_data(src)

    node_data = data.data[src]
    if verbose:
        print(f"Downloading {node_data.name}")

    can_have_children = node_data.type in {
        LDataNodeType.account_root,
        LDataNodeType.dir,
        LDataNodeType.mount,
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
        raise HTTPException(
            f"failed to fetch presigned url(s) for path {src} with code"
            f" {res.status_code}: {res.json()['error']}"
        )

    json_data = res.json()
    if can_have_children:
        dir_data: _GetSignedUrlsRecursiveData = json_data["data"]

        if dest.exists() and not normalized.endswith("/"):
            dest = dest / node_data.name

        try:
            dest.mkdir(exist_ok=True)
        except FileNotFoundError as e:
            raise ValueError(f"No such download destination {dest}")
        except (FileExistsError, NotADirectoryError) as e:
            raise ValueError(f"Download destination {dest} is not a directory")

        unconfirmed_jobs: List[_DownloadJob] = []
        confirmed_jobs: List[_DownloadJob] = []
        rejected_jobs: Set[Path] = set()

        for rel_path, url in dir_data["urls"].items():
            unconfirmed_jobs.append(_DownloadJob(url, dest / rel_path))

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
        if progress == Progress.total:
            num_bars = 0
            show_total_progress = True
        else:
            num_bars = min(_get_max_workers(), num_files)
            show_total_progress = True

        with _TransferStateManager() as manager:
            progress_bars: _ProgressBars
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
                with ProcessPoolExecutor(max_workers=_get_max_workers()) as executor:
                    total_bytes = sum(
                        executor.map(
                            _download_file,
                            confirmed_jobs,
                            repeat(progress_bars),
                        )
                    )

                end = time.monotonic()
    else:
        file_data: _GetSignedUrlData = json_data["data"]

        num_files = 1

        if dest.exists() and dest.is_dir():
            dest = dest / node_data.name

        if progress == Progress.none:
            num_bars = 0
        else:
            num_bars = 1

        with _TransferStateManager() as manager:
            progress_bars: _ProgressBars
            with closing(
                manager.ProgressBars(
                    num_bars,
                    show_total_progress=False,
                    verbose=verbose,
                )
            ) as progress_bars:
                start = time.monotonic()
                total_bytes = _download_file(
                    _DownloadJob(file_data["url"], dest),
                    progress_bars,
                )
                end = time.monotonic()

    total_time = end - start

    if verbose:
        print(dedent(f"""
				Download Complete
				Time Elapsed: {_human_readable_time(total_time)}
				Files Downloaded: {num_files} ({with_si_suffix(total_bytes)})
				"""))


# dest will always be a path which includes the copied file as its leaf
# e.g. download_file("a/b.txt", Path("c/d.txt")) will copy the content of 'b.txt' into 'd.txt'
def _download_file(
    job: _DownloadJob,
    progress_bars: _ProgressBars,
) -> int:
    # todo(ayush): benchmark parallelized downloads using the range header
    with open(job.dest, "wb") as f:
        res = tinyrequests.get(job.signed_url, stream=True)

        total_bytes = res.headers.get("Content-Length")
        assert total_bytes is not None, "Must have a content-length header"

        with _get_free_index(progress_bars) as pbar_index:
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
                    f" in {_human_readable_time(end - start)}"
                )

        return int(total_bytes)
