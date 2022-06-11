import requests

from latch_cli.config.latch import LatchConfig
from latch_cli.utils import _normalize_remote_path, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def rm(remote_path: str):
    """Deletes an entity on Latch

    Args:
        remote_path:   A valid path to a remote destination, of the form

                            [latch://] [/] dir_1/dir_2/.../dir_n/entity_name,

                       where entity_name is the name of the entity to be removed.

    This function will remove the entity at the remote path specified recursively
    (like rm -r on POSIX systems), and will error if the remote path specified is
    invalid or if the entity doesn't exist.

    Example: ::

        rm("sample.txt") # sample.txt exists

            Removes the existing file sample.txt from Latch.

        rm("latch:///dir1/dir2") # dir1/dir2/ exists and is nonempty

            Removes the directory dir1/dir2 along with all of its contents.

        rm("/dir1/dir3/dir2/doesnt_exist.txt") # doesnt_exist.txt doesn't exist

            Will throw an error, as this operation tries to remove a file
            that doesn't exist.
    """
    token = retrieve_or_login()
    remote_path = _normalize_remote_path(remote_path)

    data = {"filename": remote_path}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url=endpoints["remove"], headers=headers, json=data)

    data = response.json()
    if not data["success"]:
        raise ValueError(data["error"]["data"]["message"])
