"""Service to copy files. """

import math
from pathlib import Path

import requests
from latch.utils import retrieve_or_login

_CHUNK_SIZE = 5 * 10 ** 6  # 5 MB


def cp(local_file: str, remote_dest: str):
    """Allows movement of files between local machines and Latch.

    Args:
        local_file: valid path to a local file (can be absolute or relative)
        remote_dest: A valid path to a LatchData file. The path must be
            absolute. The path can be optionally prefixed with `latch://`.

    This function will initiate a `multipart upload`_ directly with AWS S3. The
    upload URLs are retrieved and presigned using credentials proxied through
    Latch's APIs.

    Example: ::

        cp("sample.fa", "latch://sample.fa")
        cp("sample.fa", "latch://new_name/sample.fa")

        # You can also drop the `latch://` prefix...
        cp("sample.fa", "/samples/sample.fa")

    .. _multipart upload:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    """

    local_file = Path(local_file).resolve()
    if local_file.exists() is not True:
        raise ValueError(f"{local_file} must exist.")

    if remote_dest[:8] != "latch://":
        if remote_dest[0] == "/":
            remote_dest = f"latch:/{remote_dest}"
        else:
            raise ValueError(f"{remote_dest} must be prefixed with 'latch://' or '/'")

    token = retrieve_or_login()

    with open(local_file, "rb") as f:
        f.seek(0, 2)
        total_bytes = f.tell()
        f.seek(0, 0)

    nrof_parts = math.ceil(total_bytes / _CHUNK_SIZE)

    data = {
        "dest_path": remote_dest,
        "node_name": local_file.name,
        "content_type": "text/plain",
        "nrof_parts": nrof_parts,
    }
    url = "https://nucleus.latch.bio/sdk/initiate-multipart-upload"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json=data)

    r_json = response.json()
    path = r_json["path"]
    upload_id = r_json["upload_id"]
    urls = r_json["urls"]

    parts = []
    print(f"\t{local_file.name} -> {remote_dest}")
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
        with open(local_file, "rb") as f:
            f.seek(i * _CHUNK_SIZE, 0)
            resp = requests.put(url, f.read(_CHUNK_SIZE))
            etag = resp.headers["ETag"]
            parts.append({"ETag": etag, "PartNumber": i + 1})

    data = {"path": path, "upload_id": upload_id, "parts": parts}
    url = "https://nucleus.latch.bio/sdk/complete-multipart-upload"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json=data)
