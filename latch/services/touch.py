import requests

from latch.config.latch import ENV, LatchConfig
from latch.utils import retrieve_or_login

config = LatchConfig(ENV)
endpoints = config.sdk_endpoints


def touch(remote_file):
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"filename": remote_file}

    response = requests.post(endpoints["touch"], json=data, headers=headers)
    json_data = response.json()
    if not json_data["success"]:
        raise ValueError(data["error"]["data"]["message"])
