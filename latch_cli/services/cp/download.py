import time
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from pathlib import Path
from typing import Dict, List, TypedDict

import click

from latch_cli import tinyrequests
from latch_cli.config.latch import config as latch_config
from latch_cli.services.cp.config import CPConfig, Progress
from latch_cli.services.cp.progress import ProgressBarManager, ProgressBars
from latch_cli.services.cp.utils import (
    LDataNodeType,
    get_auth_header,
    get_node_data,
    human_readable_time,
    normalize_path,
    pluralize,
)
from latch_cli.utils import with_si_suffix


class GetSignedUrlData(TypedDict):
    url: str


class GetSignedUrlsRecursiveData(TypedDict):
    urls: Dict[str, str]


def download(
    src: str,
    dest: Path,
    config: CPConfig,
):
    normalized = normalize_path(src)
    node_data = get_node_data(normalized)

    node_has_children = node_data.type in {
        LDataNodeType.account_root,
        LDataNodeType.dir,
        LDataNodeType.mount,
    }

    if node_has_children:
        endpoint = latch_config.api.data.get_signed_urls_recursive
    else:
        endpoint = latch_config.api.data.get_signed_url

    res = tinyrequests.post(
        endpoint,
        headers=get_auth_header(),
        json={"path": normalized},
    )

    if res.status_code != 200:
        raise ValueError(f"Failed to download {src}: {res.json()['error']}")

    json_data = res.json()
    if node_has_children:
        dir_data: GetSignedUrlsRecursiveData = json_data["data"]

        if dest.exists() and not dest.is_dir():
            raise ValueError(f"{str(dest)} is not a directory")

        if dest.exists() and not normalized.endswith("/"):
            dest = dest / node_data.name

        urls: List[str] = []
        dests: List[Path] = []

        for rel_path, url in dir_data["urls"].items():
            urls.append(url)
            dests.append(dest / rel_path)

        num_files = len(urls)

        if config.progress == Progress.none:
            num_bars = 0
            show_total_progress = False
        if config.progress == Progress.total:
            num_bars = 0
            show_total_progress = True
        else:
            num_bars = min(config.max_concurrent_files, num_files)
            show_total_progress = True

        with ProgressBarManager() as manager:
            progress_bars: ProgressBars = manager.ProgressBars(
                num_bars,
                show_total_progress=show_total_progress,
                verbose=config.verbose,
            )
            progress_bars.set_total(num_files, "Copying Files")

            start = time.perf_counter()
            with ProcessPoolExecutor() as executor:
                total_bytes = sum(
                    executor.map(
                        download_file,
                        urls,
                        dests,
                        repeat(config.chunk_size),
                        repeat(progress_bars),
                    )
                )
            end = time.perf_counter()
            progress_bars.close()
    else:
        file_data: GetSignedUrlData = json_data["data"]

        num_files = 1

        if dest.exists() and dest.is_dir():
            dest = dest / node_data.name

        if config.progress == Progress.none:
            num_bars = 0
        else:
            num_bars = 1

        with ProgressBarManager() as manager:
            progress_bars: ProgressBars = manager.ProgressBars(
                num_bars,
                show_total_progress=False,
                verbose=config.verbose,
            )

            start = time.perf_counter()
            total_bytes = download_file(
                file_data["url"], dest, config.chunk_size, progress_bars
            )
            end = time.perf_counter()
            progress_bars.close()

    total_time = end - start

    click.secho(
        (
            "Downloaded"
            f" {num_files} {pluralize('file', 'files', num_files)} ({with_si_suffix(total_bytes)})"
            f" in {human_readable_time(total_time)}."
        ),
        fg="green",
    )


# dest will always be a path which includes the copied file as its leaf
# e.g. download_file("a/b.txt", Path("c/d.txt")) will copy the content of 'b.txt' into 'd.txt'
def download_file(
    signed_url: str,
    dest: Path,
    chunk_size: int,
    progress_bars: ProgressBars,
) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        res = tinyrequests.get(signed_url, stream=True)

        total_bytes = res.headers.get("Content-Length")
        if total_bytes is not None:
            pbar_index = progress_bars.get_free_task_bar_index()
            progress_bars.set(index=pbar_index, total=int(total_bytes), desc=dest.name)

            for data in res.iter_content(chunk_size):
                f.write(data)
                progress_bars.update(pbar_index, len(data))

            progress_bars.return_task_bar(pbar_index)
            progress_bars.update_total_progress(1)
            progress_bars.write(f"Copied {dest.name}")

            return int(total_bytes)
        else:
            total_bytes = 0
            for data in res.iter_content(chunk_size):
                f.write(data)
                total_bytes += len(data)

            return total_bytes
