
import requests
import webbrowser

from latch.config.latch import ENV, LatchConfig
from latch.utils import retrieve_or_login

config = LatchConfig(ENV)
endpoints = config.sdk_endpoints

_CHUNK_SIZE = 5 * 10 ** 6  # 5 MB

def open_file(remote_file: str):
    """
    Returns a URL that a user can open in console
    """
    token = retrieve_or_login()
    url = endpoints["id"]
    headers = {"Authorization": f"Bearer {token}"}
    data = {"filename": remote_file}

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 403:
        raise ValueError(
            "you need access to the latch sdk beta ~ join the waitlist @ https://latch.bio/sdk"
        )
    elif response.status_code == 401:
        raise ValueError(
            "your token has expired - please run latch login to refresh your token and try again."
        )

    try:
        json_data = response.json()
        node_id = json_data["id"]
        open_url = f"{config.console_url}/data/{node_id}"
        webbrowser.open(open_url)
    except:
        raise ValueError("Either specified file does not exist or you are trying to open a directory.")
