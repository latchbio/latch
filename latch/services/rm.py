import requests

from latch.config.latch import ENV, LatchConfig
from latch.utils import retrieve_or_login

config = LatchConfig(ENV)
endpoints = config.sdk_endpoints


def rm(filename: str):
    token = retrieve_or_login()

    if filename.startswith("latch://"):
        filename = filename[len("latch://") :]
    if not filename[0] == "/":
        filename = f"/{filename}"

    data = {"filename": filename}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url=endpoints["remove"], headers=headers, json=data)

    if response.status_code == 403:
        raise ValueError(
            "you need access to the latch sdk beta ~ join the waitlist @ https://latch.bio/sdk"
        )
    elif response.status_code == 401:
        raise ValueError(
            "your token has expired - please run latch login to refresh your token and try again."
        )

    data = response.json()
    if not data["success"]:
        raise ValueError(data["error"]["data"]["message"])
