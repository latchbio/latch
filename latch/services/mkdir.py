import requests

from latch.config.latch import ENV, LatchConfig
from latch.utils import retrieve_or_login

config = LatchConfig(ENV)
endpoints = config.sdk_endpoints


def mkdir(remote_directory):
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"directory": remote_directory}

    response = requests.post(endpoints["mkdir"], headers=headers, json=data)
    json_data = response.json()

    if not json_data["success"]:
        raise ValueError(json_data["error"]["data"]["message"])

    response = requests.post(
        endpoints["verify"], headers=headers, json={"filename": remote_directory}
    )
    json_data = response.json()

    if not json_data["success"]:
        raise ValueError(json_data["error"]["data"]["message"])
    elif not json_data["exists"]:
        raise ValueError("Unable to create directory for some reason.")
