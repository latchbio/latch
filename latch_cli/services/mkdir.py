import latch_cli.tinyrequests as tinyrequests
from latch_cli.config.latch import LatchConfig
from latch_cli.utils import _normalize_remote_path, current_workspace, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def mkdir(remote_directory):
    """Creates an empty directory on Latch

    Args:
        remote_directory:   A valid path to a remote destination, of the form

                                [latch://] [/] dir_1/dir_2/.../dir_n/dir_name,

                            where dir_name is the name of the new directory to be created.
                            Every directory in the path (dir_i) must already exist.

    This function will create a directory at the specified path in Latch. Will error if
    the path is invalid or if an upstream directory does not exist. If a directory with the
    same name already exists, this will make a new directory with an indexed name (see below).

    Example: ::

        mkdir("sample") # sample doesn't already exist

            Creates a new empty directory visible in Latch Console called sample, located in
            the root of the user's Latch filesystem

        mkdir("latch:///dir1/dir2/sample") # dir1/dir2/sample already exists

            Creates a new directory visible in Latch Console called "sample\ 1" (note the
            escaped space), located in the nested directory dir1/dir2/.

        mkdir("/dir1/doesnt_exist/dir2/sample.txt") # doesnt_exist doesn't exist

            Will throw an error, as this operation tries to create a directory
            inside of a directory that doesn't exist.
    """
    token = retrieve_or_login()
    remote_directory = _normalize_remote_path(remote_directory)

    headers = {"Authorization": f"Bearer {token}"}
    data = {"directory": remote_directory, "ws_account_id": current_workspace()}

    response = tinyrequests.post(endpoints["mkdir"], headers=headers, json=data)
    json_data = response.json()

    if not json_data["success"]:
        raise ValueError(json_data["error"]["data"]["message"])
