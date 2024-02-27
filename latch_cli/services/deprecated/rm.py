import click
import requests
from latch_sdk_config.latch import config

from latch_cli.services.cp.glob import expand_pattern
from latch_cli.utils import _normalize_remote_path, current_workspace, retrieve_or_login


<<<<<<< Updated upstream
def rm(remote_path: str, no_glob: bool = False):
=======
<<<<<<< Updated upstream
def rm(remote_path: str):
=======
def rm(remote_path: str, skip_confirmation: bool = False, no_glob: bool = False):
>>>>>>> Stashed changes
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
    to_remove = [remote_path] if no_glob else expand_pattern(remote_path)
    for path in to_remove:
        data = {"filename": path, "ws_account_id": current_workspace()}
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(config.api.data.remove, headers=headers, json=data)
=======
<<<<<<< Updated upstream
    data = {"filename": remote_path, "ws_account_id": current_workspace()}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(config.api.data.remove, headers=headers, json=data)
>>>>>>> Stashed changes

    data = response.json()
    if not data["success"]:
        raise ValueError(data["error"]["data"]["message"])
=======
    to_remove = [remote_path] if no_glob else expand_pattern(remote_path)
    if len(to_remove) == 0:
        click.echo(
            f"Could not find any files that match pattern {remote_path}",
        )
        return

    files = "\n".join(to_remove)
    if not skip_confirmation and not click.confirm(
        f"Remove the following files?\n{files}\n"
    ):
        return

    for path in to_remove:
        path = _normalize_remote_path(path)
        print(path)
        data = {"filename": path, "ws_account_id": current_workspace()}
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(config.api.data.remove, headers=headers, json=data)
        data = response.json()
        if not data["success"]:
            raise ValueError(data["error"]["data"]["message"])
        click.secho(f"Successfully deleted {path}.", fg="green")
>>>>>>> Stashed changes
