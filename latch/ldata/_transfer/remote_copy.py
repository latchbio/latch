from textwrap import dedent

import gql
from gql.transport.exceptions import TransportQueryError
from latch_sdk_gql.execute import execute

from latch.ldata.type import LDataNodeType
from latch_cli.utils.path import get_name_from_path

from .node import LatchPathError, get_node_data


def remote_copy(src: str, dst: str) -> None:
    node_data = get_node_data(src, dst, allow_resolve_to_parent=True)

    src_data = node_data.data[src]
    dst_data = node_data.data[dst]
    acc_id = node_data.acc_id

    path_by_id = {v.id: k for k, v in node_data.data.items()}

    if src_data.is_parent:
        raise LatchPathError("not found", src, acc_id)

    new_name = None
    if dst_data.is_parent:
        new_name = get_name_from_path(dst)
    elif dst_data.type in {LDataNodeType.obj, LDataNodeType.link}:
        raise LatchPathError("object already exists at path", dst, acc_id)

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

            raise LatchPathError("permission denied", path, acc_id)
        elif msg == "Refusing to make node its own parent":
            raise LatchPathError(f"is a parent of {src}", dst, acc_id)
        elif msg == "Refusing to parent node to an object node":
            raise LatchPathError(f"object exists at path", dst, acc_id)
        elif msg == "Refusing to move a share link (or into a share link)":
            raise LatchPathError(
                "is a share link",
                src if src_data.type is LDataNodeType.link else dst,
                acc_id,
            )
        elif msg.startswith("Refusing to copy account root"):
            raise LatchPathError("is an account root", src, acc_id)
        elif msg.startswith("Refusing to copy removed node"):
            raise LatchPathError("not found", src, acc_id)
        elif msg.startswith("Refusing to copy already in-transit node"):
            raise LatchPathError("copy already in progress", src, acc_id)
        elif msg == "Conflicting object in destination":
            raise LatchPathError("object exists at path", dst, acc_id)

        raise LatchPathError(str(e), src, acc_id)
