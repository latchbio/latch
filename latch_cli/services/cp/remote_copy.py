import click
import gql
from gql.transport.exceptions import TransportQueryError
from latch_sdk_gql.execute import execute

from latch_cli.services.cp.ldata_utils import LDataNodeType, get_node_data
from latch_cli.services.cp.path_utils import get_name_from_path, get_path_error


# todo(ayush): figure out how to do progress for this
def remote_copy(
    src: str,
    dest: str,
):
    click.clear()

    node_data = get_node_data(src, dest, allow_resolve_to_parent=True)

    src_data = node_data.data[src]
    dest_data = node_data.data[dest]
    acc_id = node_data.acc_id

    path_by_id = {v.id: k for k, v in node_data.data.items()}

    if src_data.is_parent:
        raise get_path_error(src, "not found", acc_id)

    new_name = None
    if dest_data.is_parent:
        new_name = get_name_from_path(dest)
    elif dest_data.type in {LDataNodeType.obj, LDataNodeType.link}:
        raise get_path_error(dest, "object already exists at path.", acc_id)

    try:
        execute(
            gql.gql("""
            mutation Copy(
                $argSrcNode: BigInt!
                $argDstParent: BigInt!
                $argNewName: String
            ) {
                ldataCopy(
                    input: {
                        argSrcNode: $argSrcNode
                        argDstParent: $argDstParent
                        argNewName: $argNewName
                    }
                ) {
                    clientMutationId
                }
            }"""),
            {
                "argSrcNode": src_data.id,
                "argDstParent": dest_data.id,
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

            raise get_path_error(path, "permission denied.", acc_id) from e
        elif msg == "Refusing to make node its own parent":
            raise get_path_error(dest, f"is a parent of {src}.", acc_id) from e
        elif msg == "Refusing to parent node to an object node":
            raise get_path_error(dest, f"object exists at path.", acc_id) from e
        elif msg == "Refusing to move a share link (or into a share link)":
            if src_data.type is LDataNodeType.link:
                path = src
            else:
                path = dest

            raise get_path_error(path, f"is a share link.", acc_id) from e
        elif msg.startswith("Refusing to copy account root"):
            raise get_path_error(src, "is an account root.", acc_id) from e
        elif msg.startswith("Refusing to copy removed node"):
            raise get_path_error(src, "not found.", acc_id) from e
        elif msg.startswith("Refusing to copy already in-transit node"):
            raise get_path_error(src, "copy already in progress.", acc_id) from e
        elif msg == "Conflicting object in destination":
            raise get_path_error(dest, "object exists at path.", acc_id) from e

        raise e

    click.echo(f"""
{click.style("Copy Requested.", fg="green")}

{click.style("Source: ", fg="blue")}{(src)}
{click.style("Destination: ", fg="blue")}{(dest)}""")
