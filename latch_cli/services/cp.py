"""Service to copy files. """

import math
import threading
from pathlib import Path

from tqdm.auto import tqdm

import latch_cli.tinyrequests as tinyrequests
from latch_cli.config.latch import LatchConfig
from latch_cli.services.mkdir import mkdir
from latch_cli.utils import _normalize_remote_path, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints

# AWS uses this value for minimum for multipart as opposed to 5 * 10 ** 6
_CHUNK_SIZE = 5 * 2 ** 20  # 5 MB

LOCK = threading.Lock()
num_files = 0
progressbars = []


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


def _cp_local_to_remote(local_source: str, remote_dest: str):
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
        tasks = []
        for sub_dir in local_source_p.iterdir():
            tasks.append(
                threading.Thread(
                    target=_cp_local_to_remote,
                    kwargs={
                        "local_source": sub_dir,
                        "remote_dest": f"{remote_dest}/{sub_dir.name}",
                    },
                )
            )

        for task in tasks:
            task.start()

        for task in tasks:
            task.join()

    else:
        _upload_file(local_source_p, remote_dest)


def _upload_file(local_source: Path, remote_dest: str):
    with open(local_source, "rb") as f:
        f.seek(0, 2)
        total_bytes = f.tell()

    nrof_parts = math.ceil(total_bytes / _CHUNK_SIZE)

    data = {
        "dest_path": remote_dest,
        "node_name": local_source.name,
        "content_type": "text/plain",
        "nrof_parts": nrof_parts,
    }

    token = retrieve_or_login()

    url = endpoints["initiate-multipart-upload"]
    headers = {"Authorization": f"Bearer {token}"}
    response = tinyrequests.post(url, headers=headers, json=data)

    response_json = response.json()
    path = response_json["path"]
    upload_id = response_json["upload_id"]
    urls = response_json["urls"]

    parts = []
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    index = 0
    while total_bytes // (1024 ** index) > 1000:
        index += 1

    unit = 1024 ** index
    total_human_readable = total_bytes // unit
    suffix = units[index]
    text = f"Copying {local_source.relative_to(Path.cwd())} -> {remote_dest}:"

    with LOCK:
        global num_files
        file_index = num_files
        num_files += 1
        progressbars.append(
            tqdm(
                total=total_human_readable,
                position=file_index,
                desc=text,
                unit=suffix,
                leave=False,
                colour="green",
            )
        )

    import requests

    for i in range(nrof_parts):

        url = urls[str(i)]
        with open(local_source, "rb") as f:
            f.seek(i * _CHUNK_SIZE, 0)
            resp = requests.put(url, f.read(_CHUNK_SIZE))
            etag = resp.headers["ETag"]
            parts.append({"ETag": etag, "PartNumber": i + 1})

        with LOCK:
            progressbars[file_index].update(_CHUNK_SIZE / unit)

    data = {"path": path, "upload_id": upload_id, "parts": parts}
    url = endpoints["complete-multipart-upload"]
    headers = {"Authorization": f"Bearer {token}"}
    response = tinyrequests.post(url, headers=headers, json=data)


def _cp_remote_to_local(remote_source: str, local_dest: str):
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

    local_dest_p = Path(local_dest).resolve()
    if local_dest_p.is_dir():
        last_slash = remote_source.rfind("/")
        local_dest_p = local_dest_p / remote_source[last_slash + 1 :]

    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"source_path": remote_source}

    url = endpoints["download"]
    response = tinyrequests.post(url, headers=headers, json=data)

    response_data = response.json()

    if response.status_code == 400:
        raise ValueError(response_data["error"]["data"]["message"])

    is_dir = response_data["dir"]

    if is_dir:
        output_dir = local_dest_p
        output_dir.mkdir(exist_ok=True)
        _cp_remote_to_local_dir(output_dir, response_data)
    else:
        url = response_data["url"]
        with tinyrequests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_dest_p, "wb") as f:
                for chunk in r.iter_content(chunk_size=_CHUNK_SIZE):
                    f.write(chunk)


def _cp_remote_to_local_dir_helper(output_dir: Path, name: str, response_data: dict):
    if response_data["dir"]:
        sub_dir = output_dir.resolve().joinpath(name)
        sub_dir.mkdir(exist_ok=True)
        _cp_remote_to_local_dir(sub_dir, response_data)
    else:
        url = response_data["url"]
        with tinyrequests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(output_dir.resolve().joinpath(name), "wb") as f:
                for chunk in r.iter_content(chunk_size=_CHUNK_SIZE):
                    f.write(chunk)


def _cp_remote_to_local_dir(output_dir: Path, response_data: dict):
    urls = response_data["url"]
    tasks = []
    for name in urls:
        tasks.append(
            threading.Thread(
                target=_cp_remote_to_local_dir_helper,
                kwargs={
                    "output_dir": output_dir,
                    "name": name,
                    "response_data": urls[name],
                },
            )
        )

    for task in tasks:
        task.start()

    for task in tasks:
        task.join()


def cp(source_file: str, destination_file: str):
    if not source_file.startswith("latch://") and (
        destination_file.startswith("latch://shared")
        or destination_file.startswith("latch://account")
        or destination_file.startswith("latch:///")
    ):
        _cp_local_to_remote(source_file, destination_file)
        for progressbar in progressbars:
            progressbar.close()
    elif (
        source_file.startswith("latch:///")
        or source_file.startswith("latch://shared")
        or source_file.startswith("latch://account")
    ) and not destination_file.startswith("latch://"):
        _cp_remote_to_local(source_file, destination_file)
    else:
        raise ValueError(
            "latch cp can only be used to either copy remote -> local or local ->"
            " remote"
        )
