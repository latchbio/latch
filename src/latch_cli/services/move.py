from textwrap import dedent

import click
import gql
from gql.transport.exceptions import TransportQueryError
from latch_sdk_gql.execute import execute

from latch.ldata._transfer.node import get_node_data as _get_node_data
from latch.ldata.type import LDataNodeType
from latch_cli.services.cp.glob import expand_pattern
from latch_cli.utils.path import get_name_from_path, get_path_error, is_remote_path


def move(
    src: str,
    dest: str,
    *,
    no_glob: bool = False,
):
    if not is_remote_path(src) or not is_remote_path(dest):
        click.secho(
            f"`latch mv` cannot be used for local file operations. Please make sure"
            f" all of your input paths are remote (beginning with `latch://`)",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if no_glob:
        srcs = [src]
    else:
        srcs = expand_pattern(src)

    if len(srcs) == 0:
        click.echo(f"Could not find any files that match pattern {src}. Exiting.")
        raise click.exceptions.Exit(0)

    try:
        node_data = _get_node_data(*srcs, dest, allow_resolve_to_parent=True)
    except Exception as e:
        click.echo(str(e))
        raise click.exceptions.Exit(1) from e

    dest_data = node_data.data[dest]
    acc_id = node_data.acc_id

    multi_src = len(srcs) > 1

    if not (dest_data.exists() or dest_data.is_direct_parent()):
        click.echo(get_path_error(dest, "not found", acc_id))
        raise click.exceptions.Exit(1)
    elif multi_src and not dest_data.exists():
        click.secho(
            f"Remote destination {dest} does not exist. Cannot move multiple source"
            " files to a destination that does not exist.",
            fg="red",
        )
        raise click.exceptions.Exit(1)
    elif multi_src and dest_data.type in {LDataNodeType.obj, LDataNodeType.link}:
        click.secho(
            f"Remote destination {dest} is not a directory. Cannot move multiple source"
            " files to a destination that is not a directory.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    path_by_id = {v.id: k for k, v in node_data.data.items()}

    for s in srcs:
        src_data = node_data.data[s]

        if not src_data.exists():
            click.echo(get_path_error(s, "not found", acc_id))
            raise click.exceptions.Exit(1)

        new_name = None
        if dest_data.is_direct_parent():
            new_name = get_name_from_path(dest)
        elif dest_data.type in {LDataNodeType.obj, LDataNodeType.link}:
            click.echo(get_path_error(dest, "object already exists at path.", acc_id))
            raise click.exceptions.Exit(1)

        try:
            execute(
                gql.gql("""
                    mutation Move(
                        $argNode: BigInt!
                        $argDestParent: BigInt!
                        $argNewName: String
                    ) {
                        ldataMove(
                            input: {
                                argNode: $argNode
                                argDestParent: $argDestParent
                                argNewName: $argNewName
                            }
                        ) {
                            clientMutationId
                        }
                    }
                """),
                {
                    "argNode": src_data.id,
                    "argDestParent": dest_data.id,
                    "argNewName": new_name,
                },
            )
        except TransportQueryError as e:
            if e.errors is None or len(e.errors) == 0:
                raise e

            msg: str = e.errors[0]["message"]

            if msg.startswith("Permission denied on node"):
                node_id = msg.rsplit(" ", 1)[1]
                path = path_by_id[node_id]

                click.echo(get_path_error(path, "permission denied.", acc_id))
                raise click.exceptions.Exit(1) from e
            elif msg == "Refusing to make node its own parent":
                click.echo(get_path_error(dest, f"is a parent of {s}.", acc_id))
                raise click.exceptions.Exit(1) from e
            elif msg == "Refusing to parent node to an object node":
                click.echo(get_path_error(dest, f"object exists at path.", acc_id))
                raise click.exceptions.Exit(1) from e
            elif msg == "Refusing to move a share link (or into a share link)":
                if src_data.type is LDataNodeType.link:
                    path = s
                else:
                    path = dest

                click.echo(get_path_error(path, f"is a share link.", acc_id))
                raise click.exceptions.Exit(1) from e
            elif msg.startswith("Refusing to move account root"):
                click.echo(get_path_error(s, "is an account root.", acc_id))
                raise click.exceptions.Exit(1) from e
            elif msg.startswith("Refusing to move removed node"):
                click.echo(get_path_error(s, "not found.", acc_id))
                raise click.exceptions.Exit(1) from e
            elif msg.startswith("Refusing to move already moved node"):
                click.echo(
                    get_path_error(
                        s,
                        "copy in progress. Please wait until the node has finished"
                        " copying before moving.",
                        acc_id,
                    )
                )
                raise click.exceptions.Exit(1) from e
            elif msg == "Conflicting object in destination":
                click.echo(get_path_error(dest, "object exists at path.", acc_id))
                raise click.exceptions.Exit(1) from e
            elif msg.startswith("Refusing to do noop move"):
                click.echo(get_path_error(dest, "cannot move node to itself.", acc_id))
                raise click.exceptions.Exit(1) from e
            else:
                raise e

    if len(srcs) == 1:
        src_str = f'{click.style("Source: ", fg="blue")}{srcs[0]}'
    else:
        src_str = "\n".join(
            [click.style("Sources: ", fg="blue"), *[f"    {s}" for s in srcs]]
        )

    click.echo(dedent(f"""
        {click.style("Move Succeeded.", fg="green")}

        __srcs__
        {click.style("Destination: ", fg="blue")}{(dest)}
        """).replace("__srcs__", src_str).strip())
