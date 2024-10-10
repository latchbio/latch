import asyncio
import queue
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Literal, Optional, TypedDict

import click
import requests
import requests.adapters
import tqdm

from ....utils import get_auth_header, human_readable_time, urljoins, with_si_suffix
from ....utils.path import normalize_path
from ..glob import expand_pattern
from .worker import Work, worker

http_session = requests.Session()

_adapter = requests.adapters.HTTPAdapter(
    max_retries=requests.adapters.Retry(
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1,
        allowed_methods=["GET", "PUT", "POST"],
    )
)
http_session.mount("https://", _adapter)
http_session.mount("http://", _adapter)


class GetSignedUrlData(TypedDict):
    url: str


class GetSignedUrlsRecursiveData(TypedDict):
    urls: Dict[str, str]


def download(
    srcs: List[str],
    dest: Path,
    progress: Literal["none", "total", "tasks"],
    verbose: bool,
    force: bool,
    expand_globs: bool,
    cores: Optional[int],
    chunk_size_mib: Optional[int],
):
    if cores is None:
        cores = 4
    if chunk_size_mib is None:
        chunk_size_mib = 16

    start = time.monotonic()

    if not dest.parent.exists():
        click.secho(
            f"Invalid copy destination {dest}. Parent directory {dest.parent} does"
            " not exist.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if len(srcs) > 1 and not (dest.exists() and dest.is_dir()):
        click.secho(
            f"Copy destination {dest} does not exist. Multi-source copies must write to"
            " a pre-existing directory.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    from latch.ldata.path import _get_node_data
    from latch.ldata.type import LDataNodeType

    all_node_data = _get_node_data(*srcs)
    work_queue = queue.Queue()
    total = 0

    if expand_globs:
        new_srcs = []
        for src in srcs:
            new_srcs.extend(expand_pattern(src))

        srcs = new_srcs

    # todo(ayush): parallelize
    for src in srcs:
        node_data = all_node_data.data[src]
        normalized = normalize_path(src)

        can_have_children = node_data.type in {
            LDataNodeType.account_root,
            LDataNodeType.dir,
            LDataNodeType.mount,
            LDataNodeType.mount_gcp,
            LDataNodeType.mount_azure,
        }

        if not can_have_children:
            endpoint = "https://nucleus.latch.bio/ldata/get-signed-url"
        else:
            endpoint = "https://nucleus.latch.bio/ldata/get-signed-urls-recursive"

        res = http_session.post(
            endpoint,
            headers={"Authorization": get_auth_header()},
            json={"path": normalized},
        )

        json = res.json()

        if not can_have_children:
            gsud: GetSignedUrlData = json["data"]
            total += 1

            work_dest = dest
            if dest.exists() and dest.is_dir():
                work_dest = dest / node_data.name

            if (
                work_dest.exists()
                and not force
                and not click.confirm(
                    f"Copy destination path {work_dest} already exists and its contents"
                    " may be overwritten. Proceed?"
                )
            ):
                return

            try:
                work_dest.unlink(missing_ok=True)
            except OSError:
                shutil.rmtree(work_dest)

            work_queue.put(Work(gsud["url"], work_dest, chunk_size_mib))
        else:
            gsurd: GetSignedUrlsRecursiveData = json["data"]
            total += len(gsurd["urls"])

            work_dest = dest
            if dest.exists() and not normalized.endswith("/"):
                work_dest = dest / node_data.name

            if (
                work_dest.exists()
                and work_dest.is_dir()
                and not force
                and not click.confirm(
                    f"Copy destination path {work_dest} already exists and its contents"
                    " may be overwritten. Proceed?"
                )
            ):
                return

            for rel, url in gsurd["urls"].items():
                res = work_dest / rel

                for parent in res.parents:
                    try:
                        parent.mkdir(exist_ok=True, parents=True)
                        break
                    except NotADirectoryError:  # somewhere up the tree is a file
                        continue
                    except FileExistsError:
                        parent.unlink()
                        break

                # todo(ayush): use only one mkdir call
                res.parent.mkdir(exist_ok=True, parents=True)

                work_queue.put(Work(url, work_dest / rel, chunk_size_mib))

    tbar = tqdm.tqdm(
        total=total,
        leave=False,
        colour="green",
        smoothing=0,
        unit="B",
        unit_scale=True,
        disable=progress == "none",
    )

    workers = min(total, cores)
    with ThreadPoolExecutor(workers) as exec:
        futs = [
            exec.submit(worker, work_queue, tbar, progress == "tasks", verbose)
            for _ in range(workers)
        ]

    total_bytes = 0
    for fut in as_completed(futs):
        total_bytes += fut.result()

    tbar.clear()
    total_time = time.monotonic() - start

    if progress != "none":
        click.echo(dedent(f"""\
            {click.style("Download Complete", fg="green")}
            {click.style("Time Elapsed:", fg="blue")}     {human_readable_time(total_time)}
            {click.style("Files Downloaded:", fg="blue")} {total} ({with_si_suffix(total_bytes)})\
        """))
