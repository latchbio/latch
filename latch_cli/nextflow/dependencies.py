import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor
from ctypes import c_int
from multiprocessing.managers import SyncManager
from pathlib import Path
from urllib.parse import urljoin

import boto3
import click

from latch_cli import tinyrequests
from latch_cli.utils import dedent


def _do_download(
    url: str,
    output_path: Path,
    total_count: int,
    counter,
    lock,
):  # todo(ayush): figure out the right type annotation for counter/lock
    res = tinyrequests.get(url)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(res.content)

    with lock:
        counter.value += 1
        progress_str = f"{counter.value}/{total_count}"

        click.echo("\x1b[0K", nl=False)
        click.secho(progress_str, dim=True, italic=True, nl=False)
        click.echo(f"\x1b[{len(progress_str)}D", nl=False)


def download_nf_jars(pkg_root: Path):
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket("latch-public")

    subdir = "nextflow-v2/"
    objects = list(bucket.objects.filter(Prefix=f"{subdir}.nextflow/"))

    click.secho("  Downloading Nextflow binaries: \x1b[?25l", italic=True, nl=False)

    with SyncManager() as man:
        counter = man.Value(c_int, 0)
        lock = man.Lock()
        with ProcessPoolExecutor() as exec:
            for obj in objects:
                url = urljoin(
                    "https://latch-public.s3.us-west-2.amazonaws.com/", obj.key
                )
                obj_path = pkg_root / ".latch" / obj.key[len(subdir) :]
                print(obj_path)

                exec.submit(_do_download, url, obj_path, len(objects), counter, lock)

    click.echo("\x1b[0K", nl=False)
    click.secho("Done. \x1b[?25h", italic=True)


def ensure_nf_dependencies(pkg_root: Path, *, force_redownload: bool = False):
    nf_executable = pkg_root / ".latch" / "bin" / "nextflow"
    nf_jars = pkg_root / ".latch" / ".nextflow"

    if force_redownload:
        nf_executable.unlink(missing_ok=True)
        if nf_jars.exists():
            shutil.rmtree(nf_jars)

    if not nf_executable.exists():
        res = tinyrequests.get(
            "https://latch-public.s3.us-west-2.amazonaws.com/nextflow-v2/nextflow"
        )
        nf_executable.parent.mkdir(parents=True, exist_ok=True)

        nf_executable.write_bytes(res.content)
        nf_executable.chmod(0o700)

    if not nf_jars.exists():
        download_nf_jars(pkg_root)
