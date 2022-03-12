"""Service to list files in a remote directory."""

from typing import Dict, List

import requests

from latch.config.latch import ENV, LatchConfig
from latch.utils import retrieve_or_login

config = LatchConfig(ENV)
endpoints = config.sdk_endpoints


def ls(remote_directory: str) -> List[Dict[str, str]]:
    if remote_directory.startswith("latch://"):
        remote_directory = remote_directory[len("latch://") :]
    if not remote_directory.startswith("/"):
        remote_directory = f"/{remote_directory}"

    url = endpoints["list-files"]
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"directory": remote_directory}

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 403:
        raise ValueError(
            "you need access to the latch sdk beta ~ join the waitlist @ https://latch.bio/sdk"
        )
    elif response.status_code == 500:
        raise ValueError(f"the directory does not exist.")

    json_data = response.json()

    output = []
    for i in json_data:
        name_data = json_data[i]
        output.append(name_data)

    output.sort(key=lambda x: x["name"])
    output.sort(key=lambda x: x["type"])

    return output
