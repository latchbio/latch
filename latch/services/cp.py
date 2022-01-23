import math
from pathlib import Path

import requests
from latch.config import UserConfig


def cp(local_file: str, remote_dest: str):

    user_conf = UserConfig()
    token = user_conf.token

    local_file = Path(local_file).resolve()
    if local_file.exists() is not True:
        raise ValueError(f"{local_file} must exist.")

    if remote_dest[:8] != "latch://":
        if remote_dest[0] == "/":
            remote_dest = f"latch:/{remote_dest}"
        else:
            raise ValueError(f"{remote_dest} must be prefixed with 'latch://' or '/'")

    with open(local_file, "rb") as f:
        f.seek(0, 2)
        total_bytes = f.tell()
        f.seek(0, 0)

    chunk_size = 10000000
    nrof_parts = math.ceil(total_bytes / chunk_size)

    data = {
        "dest_path": remote_dest,
        "node_name": local_file.name,
        "content_type": "text/plain",
        "nrof_parts": nrof_parts,
    }
    url = "https://nucleus.ligma.ai/sdk/initiate-multipart-upload"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json=data)

    r_json = response.json()
    path = r_json["path"]
    upload_id = r_json["upload_id"]
    urls = r_json["urls"]

    parts = []
    for i in range(nrof_parts):
        url = urls[str(i)]
        with open(local_file, "rb") as f:
            f.seek(0, i * chunk_size)
            resp = requests.put(url, f.read(chunk_size))
            etag = resp.headers["ETag"]
            parts.append({"ETag": etag, "PartNumber": i + 1})

    data = {"path": path, "upload_id": upload_id, "parts": parts}
    url = "https://nucleus.ligma.ai/sdk/complete-multipart-upload"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json=data)
