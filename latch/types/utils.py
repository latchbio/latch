import re
from pathlib import Path
from typing import Union
from urllib.parse import urlparse

import gql
from latch_sdk_gql.execute import execute


def _is_valid_url(raw_url: Union[str, Path]) -> bool:
    """A valid URL (as a source or destination of a LatchFile) must:
    * contain a latch or s3 scheme
    * contain an absolute path
    """
    try:
        parsed = urlparse(str(raw_url))
    except ValueError:
        return False
    if parsed.scheme not in ("latch", "s3"):
        return False
    if parsed.path != "" and not parsed.path.startswith("/"):
        return False
    return True


is_absolute_node_path = re.compile(r"^(latch)?://(?P<node_id>\d+).node(/)?$")

def format_path(path: str) -> str:
    match = is_absolute_node_path.match(path)

    if match is None:
        return path

    node_id = match.group("node_id")

    data = execute(
        gql.gql("""
        query ldataGetPathQ($id: BigInt!) {
            ldataGetPath(argNodeId: $id)
            ldataOwner(argNodeId: $id)
        }
        """),
        {"id": node_id},
    )

    raw_path = data["ldataGetPath"]
    owner = data["ldataOwner"]

    if raw_path is None:
        return path

    path_split = raw_path.split("/")

    if path_split[0] == "mount":
        mount_name = path_split[1]
        fpath = "/".join(path_split[2:])
        return f"latch://{mount_name}.mount/{fpath}"

    if path_split[0] == "account_root":
        fpath = "/".join(path_split[2:])
        return f"latch://{owner}.account/{fpath}"

    return path
