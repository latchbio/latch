import click
import gql
from latch_sdk_gql.execute import execute

from latch.ldata._transfer.node import get_node_data as _get_node_data
from latch.ldata.path import LatchPathError
from latch_cli.services.cp.glob import expand_pattern


def rmr(
    remote_path: str,
    skip_confirmation: bool = False,
    no_glob: bool = False,
    verbose: bool = False,
):
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
    to_remove = [remote_path] if no_glob else expand_pattern(remote_path)
    if len(to_remove) == 0:
        click.echo(
            f"Could not find any files that match pattern {remote_path}",
        )
        return

    msg = (
        "Remove the following file(s)?\n" + "\n".join(to_remove)
        if verbose
        else f"Remove {len(to_remove)} file/dir(s)?"
    )
    if not skip_confirmation and not click.confirm(msg):
        return

    try:
        node_data = _get_node_data(*to_remove).data
    except LatchPathError as e:
        click.secho(str(e), fg="red")
        raise click.exceptions.Exit(1) from e

    for path in to_remove:
        execute(
            gql.gql("""
                mutation LatchCLIRmr($argNodeId: BigInt!) {
                    ldataRmr(input: {argNodeId: $argNodeId}) {
                        clientMutationId
                    }
                }
            """),
            {"argNodeId": node_data[path].id},
        )
        if verbose:
            click.secho(f"Successfully deleted {path}.", fg="green")
