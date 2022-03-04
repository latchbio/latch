"""Service to list files in a remote directory."""

from typing import Optional
import requests
from latch.utils import retrieve_or_login

def _str_none(s: Optional[str]) -> str:
    if s is None:
        return "-"
    return str(s)

def ls(remote_directory: str, padding: int = 3):
    url = "https://nucleus.sugma.ai/sdk/list"
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"directory": remote_directory}

    response = requests.post(url, headers=headers, json=data)
    json_data = response.json()

    
    # used for pretty printing
    # Initial values are the number of characters in the output header
    max_lengths = {
        "name": len("Name") + padding, 
        "content_type": len("Content Type") + padding, 
        "content_size": len("Content Size") + padding, 
        "modify_time": len("Modify Time") + padding,
    }

    output = []
    for i in json_data:
        name_data = json_data[i]
        name = _str_none(name_data["name"])
        t = _str_none(name_data["type"])
        if t == "dir" and name[-1] != "/":
            name = name_data["name"] = f"{name}/"
        content_type = _str_none(name_data["content_type"])
        content_size = _str_none(name_data["content_size"])
        modify_time = _str_none(name_data["modify_time"])

        output.append((name, t, content_type, content_size, modify_time))

        for i in max_lengths:
            max_lengths[i] = max(max_lengths[i], padding + len(_str_none(name_data[i])))

    output.sort()
    output.sort(key=lambda x: x[1])

    return output, max_lengths
