from dataclasses import dataclass
from enum import Enum
from typing import TypedDict

import gql

from latch_cli.services.cp.exceptions import PathResolutionError


class LDataNodeType(str, Enum):
    account_root = "account_root"
    dir = "dir"
    obj = "obj"
    mount = "mount"
    link = "link"


class FinalLinkTargetPayload(TypedDict):
    type: str
    name: str


class LdataNodePayload(TypedDict):
    finalLinkTarget: FinalLinkTargetPayload


class LdataResolvePathToNodePayload(TypedDict):
    path: str
    ldataNode: LdataNodePayload


@dataclass(frozen=True)
class GetNodeDataResult:
    name: str
    type: LDataNodeType
    is_parent: bool


def get_node_data(
    remote_path: str, *, allow_resolve_to_parent: bool = False
) -> GetNodeDataResult:
    from latch.gql._execute import execute

    res: LdataResolvePathToNodePayload = execute(
        gql.gql("""
        query GetNodeType($path: String!) {
            ldataResolvePathToNode(path: $path) {
                path
                ldataNode {
                    finalLinkTarget {
                        name
                        type
                    }
                }
            }
        }
    """),
        {"path": remote_path},
    )["ldataResolvePathToNode"]

    try:
        final_link_target = res["ldataNode"]["finalLinkTarget"]
        remaining = res["path"]

        if not allow_resolve_to_parent and remaining is not None and remaining != "":
            raise ValueError("Node cannot be resolved in this workspace")

        is_parent = remaining is not None and remaining != ""

        if remaining is not None and "/" in remaining:
            raise ValueError("Node cannot be resolved in this workspace")

        return GetNodeDataResult(
            name=final_link_target["name"],
            type=LDataNodeType(final_link_target["type"].lower()),
            is_parent=is_parent,
        )
    except (TypeError, ValueError) as e:
        raise PathResolutionError(
            f"Cannot resolve {remote_path} in this workspace. Ensure that you are in"
            " the correct workspace, that the node exists, and that you have correct"
            " permissions."
        ) from e
