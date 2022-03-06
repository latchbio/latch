"""Service to copy files. """

import math
from pathlib import Path

import requests
from latch.utils import retrieve_or_login

_CHUNK_SIZE = 5 * 10 ** 6  # 5 MB


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

    local_source = Path(local_source).resolve()
    if local_source.exists() is not True:
        raise ValueError(f"{local_source} must exist.")

    if remote_dest[:9] != "latch:///":
        if remote_dest[0] == "/":
            remote_dest = f"latch://{remote_dest}"
        else:
            raise ValueError(f"{remote_dest} must be prefixed with 'latch:///' or '/'")

    token = retrieve_or_login()

    with open(local_source, "rb") as f:
        f.seek(0, 2)
        total_bytes = f.tell()
        f.seek(0, 0)

    nrof_parts = math.ceil(total_bytes / _CHUNK_SIZE)

    data = {
        "dest_path": remote_dest,
        "node_name": local_source.name,
        "content_type": "text/plain",
        "nrof_parts": nrof_parts,
    }
    url = "https://nucleus.latch.bio/sdk/initiate-multipart-upload"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 403:
        raise PermissionError(
            "You need access to the latch sdk beta ~ join the waitlist @ https://latch.bio/sdk"
        )

    r_json = response.json()
    path = r_json["path"]
    upload_id = r_json["upload_id"]
    urls = r_json["urls"]

    parts = []
    print(f"\t{local_source.name} -> {remote_dest}")
    total_mb = total_bytes // 1000000
    for i in range(nrof_parts):

        if i < nrof_parts - 1:
            _end_char = "\r"
        else:
            _end_char = "\n"

        print(
            f"\t\tcopying part {i+1}/{nrof_parts} ~ {min(total_mb, (_CHUNK_SIZE//1000000)*(i+1))}MB/{total_mb}MB",
            end=_end_char,
            flush=True,
        )
        url = urls[str(i)]
        with open(local_source, "rb") as f:
            f.seek(i * _CHUNK_SIZE, 0)
            resp = requests.put(url, f.read(_CHUNK_SIZE))
            etag = resp.headers["ETag"]
            parts.append({"ETag": etag, "PartNumber": i + 1})

    data = {"path": path, "upload_id": upload_id, "parts": parts}
    url = "https://nucleus.latch.bio/sdk/complete-multipart-upload"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json=data)

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
    if remote_source[:9] != "latch:///":
        raise ValueError(f'{remote_source} needs to be prefixed with "latch:///"')
    
    local_dest = Path(local_dest).resolve()
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"source_path": remote_source}

    # todo(ayush): change it so we don't hardcode api endpoints
    url = "https://nucleus.latch.bio/sdk/download"
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 403:
        raise PermissionError(
            "You need access to the latch sdk beta ~ join the waitlist @ https://latch.bio/sdk"
        )
    elif response.status_code == 500:
        raise ValueError(
            f"{remote_source} does not exist."
        )
    
    print(response.status_code)

    response_data = response.json()
    url = response_data['url']
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=_CHUNK_SIZE):
                f.write(chunk)

def cp(source_file: str, destination_file: str):
    if source_file[:9] != "latch:///" and destination_file[:9] == "latch:///":
        _cp_local_to_remote(source_file, destination_file)
    elif source_file[:9] == "latch:///" and destination_file[:9] != "latch:///":
        _cp_remote_to_local(source_file, destination_file)
    else:
        raise ValueError("latch cp can only be used to either copy remote -> local or local -> remote")
