import requests

from latch_cli.config.latch import LatchConfig
from latch_cli.utils import _normalize_remote_path, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def touch(remote_file: str):
    """Creates an empty text file on Latch

    Args:
        remote_file:   A valid path to a remote destination, of the form

                            [latch://] [/] dir_1/dir_2/.../dir_n/filename,

                       where filename is the name of the new file to be created.
                       Every directory in the path (dir_i) must already exist.

    This function will create a node at the specified path in Latch, and directly
    create an empty file in AWS S3 using the boto3. It will error if the remote_path
    is invalid (i.e. if it contains a directory which doesn't exist).

    Example: ::

        touch("sample.txt")

            Creates a new empty file visible in Latch Console called sample.txt, located in
            the root of the user's Latch filesystem

        touch("latch:///dir1/dir2/sample.txt")

            Creates a new file visible in Latch Console called sample.fa, located in
            the nested directory /dir1/dir2/

        touch("/dir1/doesnt_exist/dir2/sample.txt") # doesnt_exist doesn't exist

            Will throw an error, as this operation tries to create a file inside of a
            directory that doesn't exist.
    """
    remote_file = _normalize_remote_path(remote_file)
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"filename": remote_file}

    response = requests.post(endpoints["touch"], json=data, headers=headers)
    json_data = response.json()
    if not json_data["success"]:
        raise ValueError(data["error"]["data"]["message"])
