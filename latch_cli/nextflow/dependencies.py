import shutil
from concurrent.futures import ProcessPoolExecutor
from ctypes import c_int
from multiprocessing.managers import SyncManager
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import boto3
import click
from botocore.handlers import disable_signing

from latch_cli import tinyrequests

target_version = "v1.1.3"


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
    s3_resource.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
    bucket = s3_resource.Bucket("latch-public")

    subdir = f"nextflow-v2/{target_version}/"
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
                exec.submit(_do_download, url, obj_path, len(objects), counter, lock)

    click.echo("\x1b[0K", nl=False)
    click.secho("Done. \x1b[?25h", italic=True)


def _get_current_version(nf_version_path: Path):
    if not nf_version_path.exists():
        return None

    with open(nf_version_path, "r") as f:
        return f.read().strip()


def ensure_nf_dependencies(pkg_root: Path):
    nf_version_path = pkg_root / ".latch" / "nextflow_version"
    nf_executable = pkg_root / ".latch" / "bin" / "nextflow"
    nf_jars = pkg_root / ".latch" / ".nextflow"

    current_version = _get_current_version(nf_version_path)
    if current_version != target_version:
        click.secho(f"Updating Nextflow to version {target_version}", fg="yellow")
        nf_version_path.unlink(missing_ok=True)
        nf_executable.unlink(missing_ok=True)
        if nf_jars.exists():
            shutil.rmtree(nf_jars)

    if not nf_executable.exists():
        res = tinyrequests.get(
            f"https://latch-public.s3.us-west-2.amazonaws.com/nextflow-v2/{target_version}/nextflow"
        )
        nf_executable.parent.mkdir(parents=True, exist_ok=True)

        nf_executable.write_bytes(res.content)
        nf_executable.chmod(0o700)

    if not nf_jars.exists():
        download_nf_jars(pkg_root)

    with open(nf_version_path, "w") as f:
        f.write(target_version)

    click.secho(f"Using Nextflow version {target_version}", fg="green")
