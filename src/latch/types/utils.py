import re
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse

import gql
from latch_sdk_gql.execute import execute


def is_valid_url(raw_url: Union[str, Path]) -> bool:
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

# note(aidan): not sure if anything creates these paths anymore.
# I added support for gcp/azure just in case
old_style_path = re.compile(r"^(?:(?P<account_root>account_root)|(?P<mount>mount)|(?P<mount_gcp>mount_gcp)|(?P<mount_azure>mount_azure))")

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

    raw: Optional[str] = data["ldataGetPath"]
    if raw is None:
        return path

    parts = raw.split("/")

    match = old_style_path.match(raw)
    if match is None:
        return path

    if match["mount"] is not None:
        bucket = parts[1]
        key = "/".join(parts[2:])
        return f"latch://{bucket}.mount/{key}"

    if match["mount_gcp"] is not None:
        bucket = parts[1]
        key = "/".join(parts[2:])
        return f"latch://{bucket}.mount_gcp/{key}"

    if match["mount_azure"] is not None:
        bucket = parts[1]
        key = "/".join(parts[2:])
        return f"latch://{bucket}.mount_azure/{key}"

    owner: Optional[str] = data["ldataOwner"]
    if owner is None:
        return path

    if match["account_root"] is not None:
        key = "/".join(parts[2:])
        return f"latch://{owner}.account/{key}"

    return path
