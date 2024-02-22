from textwrap import dedent

import click
import gql
from gql.transport.exceptions import TransportQueryError
from latch_sdk_gql.execute import execute

from latch.ldata.type import LDataNodeType
from latch_cli.utils.path import get_name_from_path, get_path_error

from .node import get_node_data


def remote_copy(src: str, dst: str, *, show_summary: bool = False) -> None:
    node_data = get_node_data(src, dst, allow_resolve_to_parent=True)

    src_data = node_data.data[src]
    dst_data = node_data.data[dst]
    acc_id = node_data.acc_id

    path_by_id = {v.id: k for k, v in node_data.data.items()}

    if src_data.is_parent:
        raise FileNotFoundError(get_path_error(src, "not found", acc_id))

    new_name = None
    if dst_data.is_parent:
        new_name = get_name_from_path(dst)
    elif dst_data.type in {LDataNodeType.obj, LDataNodeType.link}:
        raise FileExistsError(
            get_path_error(dst, "object already exists at path.", acc_id)
        )

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
                "argDstParent": dst_data.id,
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

            raise ValueError(get_path_error(path, "permission denied.", acc_id))
        elif msg == "Refusing to make node its own parent":
            raise ValueError(get_path_error(dst, f"is a parent of {src}.", acc_id))
        elif msg == "Refusing to parent node to an object node":
            raise ValueError(get_path_error(dst, f"object exists at path.", acc_id))
        elif msg == "Refusing to move a share link (or into a share link)":
            raise ValueError(
                get_path_error(
                    src if src_data.type is LDataNodeType.link else dst,
                    f"is a share link.",
                    acc_id,
                )
            )
        elif msg.startswith("Refusing to copy account root"):
            raise ValueError(get_path_error(src, "is an account root.", acc_id))
        elif msg.startswith("Refusing to copy removed node"):
            raise ValueError(get_path_error(src, "not found.", acc_id))
        elif msg.startswith("Refusing to copy already in-transit node"):
            raise ValueError(get_path_error(src, "copy already in progress.", acc_id))
        elif msg == "Conflicting object in destination":
            raise ValueError(get_path_error(dst, "object exists at path.", acc_id))

        raise ValueError(get_path_error(src, str(e), acc_id))

    if show_summary:
        click.echo(dedent(f"""
            {click.style("Copy Requested.", fg="green")}
            {click.style("Source: ", fg="blue")}{(src)}
            {click.style("Destination: ", fg="blue")}{(dst)}"""))
