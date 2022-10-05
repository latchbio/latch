"""Service to copy files. """

import concurrent.futures as cf
import json
import math
import threading
from pathlib import Path

import tqdm as _tqdm
from tqdm.auto import tqdm

import latch_cli.tinyrequests as tinyrequests
from latch_cli.config.latch import LatchConfig
from latch_cli.constants import FILE_CHUNK_SIZE
from latch_cli.services.mkdir import mkdir
from latch_cli.services.touch import touch
from latch_cli.utils import _normalize_remote_path, current_workspace, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints

# tqdm progress bars aren't thread safe so restrict so that only one can update at a time
IO_LOCK = threading.Lock()


def _dir_exists(remote_dir: str) -> bool:
    remote_dir = _normalize_remote_path(remote_dir)

    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"filename": remote_dir}
    response = tinyrequests.post(url=endpoints["verify"], headers=headers, json=data)
    try:
        assert response.status_code == 200
    except:
        raise ValueError(f"{response.content}")
    return response.json()["exists"]


def _upload(
    local_source: str,
    remote_dest: str,
    executor: cf.ThreadPoolExecutor,
):
    """Allows movement of files from local machines -> Latch.

    Args:
        local_source:  A valid path to a local file (can be absolute or relative).
        remote_dest:   A valid path to a LatchData file. The path must be absolute
                       and prefixed with `latch://`. If a directory in the path
                       doesn't exist, that directory and everything following it
                       becomes the file name - see below.

    This function will initiate a `multipart upload`_ directly with AWS S3. The
    upload URLs are retrieved and presigned using credentials proxied through
    Latch's APIs.

    Example: ::

        cp("sample.fa", "latch:///sample.fa")

            Creates a new file visible in Latch Console called sample.fa, located in
            the root of the user's Latch filesystem

        cp("sample.fa", "latch:///dir1/dir2/sample.fa")

            Creates a new file visible in Latch Console called sample.fa, located in
            the nested directory /dir1/dir2/

        cp("sample.fa", "latch:///dir1/doesnt_exist/dir2/sample.fa") # doesnt_exist doesn't exist

            Creates a new file visible in Latch Console called doesnt_exist/dir2/sample.fa,
            located in the directory /dir1/. Note that 'doesnt_exist' and everything
            following (including the `/`s) are part of the filename.

    .. _multipart upload:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    """

    local_source_p: Path = Path(local_source).resolve()
    if local_source_p.exists() is not True:
        raise ValueError(f"{local_source} must exist.")

    remote_dest = _normalize_remote_path(remote_dest)

    if remote_dest[-1] == "/":
        remote_dest = remote_dest[:-1]

    if local_source_p.is_dir():
        if not _dir_exists(remote_dest):
            mkdir(remote_directory=remote_dest)
        for sub_dir in local_source_p.iterdir():
            # do files in serial for now to prevent deadlocks
            _upload(sub_dir, f"{remote_dest}/{sub_dir.name}", executor)
    else:
        _upload_file(local_source_p, remote_dest, executor)


def _upload_file(
    local_source: Path,
    remote_dest: str,
    executor: cf.ThreadPoolExecutor,
):
    with open(local_source, "rb") as f:
        f.seek(0, 2)
        total_bytes = f.tell()
        num_parts = math.ceil(total_bytes / FILE_CHUNK_SIZE)

    if total_bytes == 0:
        touch(remote_dest)
        return

    response = tinyrequests.post(
        endpoints["initiate-multipart-upload"],
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "ws_account_id": current_workspace(),
            "dest_path": remote_dest,
            "node_name": local_source.name,
            "content_type": "text/plain",
            "nrof_parts": num_parts,
        },
    )

    response_json = response.json()
    path = response_json["path"]
    upload_id = response_json["upload_id"]
    urls = response_json["urls"]

    if Path.cwd() in local_source.parents:
        text = f"Copying {local_source.relative_to(Path.cwd())} -> {remote_dest}:"
    else:
        text = f"Copying {local_source} -> {remote_dest}:"

    with IO_LOCK:
        progress_bar = tqdm(
            total=total_bytes,
            desc=text,
            unit="B",
            unit_scale=True,
            unit_divisor=1000,
            leave=False,
            colour="green",
        )

    parts_futures = []
    for i in range(num_parts):
        parts_futures.append(
            executor.submit(
                _upload_file_chunk,
                url=urls[str(i)],
                local_source=local_source,
                part_index=i,
                progress_bar=progress_bar,
            )
        )

    parts = []
    for part in cf.as_completed(parts_futures):
        parts.append(part.result())

    parts.sort(key=lambda res: res["PartNumber"])

    response = tinyrequests.post(
        endpoints["complete-multipart-upload"],
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "path": path,
            "upload_id": upload_id,
            "parts": parts,
        },
    )

    response.raise_for_status()

    with IO_LOCK:
        progress_bar.close()
        print(text.replace("Copying", "Copied"))


def _upload_file_chunk(
    url: str,
    local_source: Path,
    part_index: int,
    progress_bar: _tqdm.tqdm,
):
    with open(local_source, "rb") as f:
        f.seek(part_index * FILE_CHUNK_SIZE, 0)
        payload = f.read(FILE_CHUNK_SIZE)
        resp = tinyrequests.request("PUT", url, data=payload)
        etag = resp.headers["ETag"]

    with IO_LOCK:
        progress_bar.update(len(payload))

    return {"ETag": etag, "PartNumber": part_index + 1}


def _download(remote_source: str, local_dest: str, executor: cf.ThreadPoolExecutor):
    """Allows movement of files from Latch -> local machines.

    Args:
        remote_source: A valid path to an existing LatchData file. The path must
                       be absolute and prefixed with `latch://`.
        local_dest:    A (relative or absolute) path. If a directory in the path
                       doesn't exist, that directory and everything following it
                       becomes the file name - see below.

    This function will initiate a download using an authenticated and presigned
    URL directly from AWS S3.

    Example: ::

        cp("latch:///sample.fa", "sample.fa")

            Creates a new file in the user's local working directory called
            sample.fa, which has the same contents as the remote file.

        cp("latch:///dir1/dir2/sample.fa", "/dir3/dir4/sample.fa")

            Creates a new file in the local directory /dir3/dir4/ called
            sample.fa, which has the same contents as the remote file.

        cp("latch:///sample.fa", "/dir1/doesnt_exist/dir2/sample.fa")
        # doesnt_exist doesn't exist

            Creates a new file in the local directory /dir1/ called
            doesnt_exist/dir2/sample.fa, which has the same content as the
            remote file. Note the nonexistent directory is folded into the
            name of the copied file.
    """
    remote_source = _normalize_remote_path(remote_source)

    output_dir = Path(local_dest).resolve()
    if output_dir.is_dir():
        last_slash = remote_source.rfind("/")
        output_dir = output_dir / remote_source[last_slash + 1 :]

    response = tinyrequests.post(
        endpoints["download"],
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "source_path": remote_source,
            "ws_account_id": current_workspace(),
        },
    )

    response_data = response.json()
    if response_data["dir"]:
        _download_dir(output_dir, response_data, executor)
    else:
        _download_file(response_data["url"], output_dir)


def _download_dir(
    output_dir: Path,
    response_data: dict,
    executor: cf.ThreadPoolExecutor,
):
    output_dir.mkdir(exist_ok=True)
    urls = response_data["url"]

    futures = []
    for name in urls:
        sub_path = output_dir.joinpath(name)
        if urls[name]["dir"]:
            futures.append(
                executor.submit(
                    target=_download_dir,
                    output_dir=sub_path,
                    response_data=urls[name],
                    executor=executor,
                )
            )
        else:
            futures.append(
                executor.submit(
                    _download_file,
                    url=urls[name]["url"],
                    output_path=sub_path,
                )
            )


def _download_file(url: str, output_path: Path):
    with tinyrequests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(output_path.resolve(), "wb") as f:
            for chunk in r.iter_content(chunk_size=FILE_CHUNK_SIZE):
                f.write(chunk)

    # TODO(ayush) send over size info in response and properly use progress bars
    with IO_LOCK:
        print(f"Finished downloading {output_path.name}", flush=True)


def cp(source_file: str, destination_file: str):
    # by default, max_workers (i.e. maximum number of concurrent jobs) = 5 * cpu_count
    with cf.ThreadPoolExecutor() as executor:
        if not source_file.startswith("latch://") and (
            destination_file.startswith("latch://shared")
            or destination_file.startswith("latch://account")
            or destination_file.startswith("latch:///")
        ):
            _upload(source_file, destination_file, executor)
        elif (
            source_file.startswith("latch:///")
            or source_file.startswith("latch://shared")
            or source_file.startswith("latch://account")
        ) and not destination_file.startswith("latch://"):
            _download(source_file, destination_file, executor)
        else:
            raise ValueError(
                "latch cp can only be used to either copy remote -> local or local ->"
                " remote"
            )
