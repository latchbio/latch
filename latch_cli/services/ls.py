"""Service to list files in a remote directory."""

from typing import Dict, List

import latch_cli.tinyrequests as tinyrequests
from latch_cli.config.latch import LatchConfig
from latch_cli.utils import _normalize_remote_path, retrieve_or_login, with_si_suffix

config = LatchConfig()
endpoints = config.sdk_endpoints


def ls(remote_directory: str) -> List[Dict[str, str]]:
    """Lists the remote entities inside of a remote_directory

    Args:
        remote_directory:   A valid path to a remote destination, of the form

                                [latch://] [/] dir_1/dir_2/.../dir_n/dir_name,

                            where dir_name is the name of the directory to list under.
                            Every directory in the path must already exist.

    This function will list all of the entites under the remote directory specified in the
    path remote_directory. Will error if the path is invalid or the directory doesn't exist.

    Example: ::

        ls("")

            Lists all entities in the user's root directory

        ls("latch:///dir1/dir2/dir_name")

            Lists all entities inside dir1/dir2/dir_name

        touch("/dir1/doesnt_exist/dir2/") # doesnt_exist doesn't exist

            Will throw an error, as this operation tries to list under a directory which
            doesn't exist.

    """
    remote_directory = _normalize_remote_path(remote_directory)

    url = endpoints["list-files"]
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"directory": remote_directory}

    response = tinyrequests.post(url, headers=headers, json=data)

    if response.status_code == 400:
        raise ValueError(f"The directory {remote_directory} does not exist.")

    output = list(response.json().values())

    return output
