"""Service to list files in a remote directory."""

import json
from typing import Optional
import requests
from latch.utils import retrieve_or_login


def ls(remote_directory: str):

    if remote_directory.startswith("latch://"):
        remote_directory = remote_directory[len("latch://") :]
    if not remote_directory.startswith("/"):
        remote_directory = f"/{remote_directory}"

    url = "https://nucleus.latch.bio/sdk/list"
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
