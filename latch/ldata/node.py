from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, TypedDict

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import gql
import graphql.language as l
from latch_sdk_gql.execute import execute
from latch_sdk_gql.utils import _name_node, _parse_selection

from latch_cli.utils.path import get_path_error, normalize_path

AccId = int


class LDataNodeType(str, Enum):
    account_root = "account_root"
    dir = "dir"
    obj = "obj"
    mount = "mount"
    link = "link"


class FinalLinkTargetPayload(TypedDict):
    id: str
    type: str
    name: str


class LdataNodePayload(TypedDict):
    finalLinkTarget: FinalLinkTargetPayload


class LdataResolvePathToNodePayload(TypedDict):
    path: str
    ldataNode: LdataNodePayload


class AccountInfoCurrentPayload(TypedDict):
    id: str


@dataclass(frozen=True)
class NodeData:
    id: str
    name: str
    type: LDataNodeType
    removed: bool
    is_parent: bool


@dataclass(frozen=True)
class GetNodeDataResult:
    acc_id: str
    data: Dict[str, NodeData]


def get_node_data(
    *remote_paths: str, allow_resolve_to_parent: bool = False
) -> GetNodeDataResult:
    normalized: Dict[str, str] = {}

    acc_sel = _parse_selection("""
        accountInfoCurrent {
            id
        }
    """)
    assert isinstance(acc_sel, l.FieldNode)

    sels: List[l.FieldNode] = [acc_sel]

    for i, remote_path in enumerate(remote_paths):
        normalized[remote_path] = normalize_path(remote_path)

        sel = _parse_selection("""
            ldataResolvePathToNode(path: {}) {
                path
                ldataNode {
                    finalLinkTarget {
                        id
                        name
                        type
                        removed
                    }
                }
            }
        """)
        assert isinstance(sel, l.FieldNode)

        val = l.StringValueNode()
        val.value = normalized[remote_path]

        args = l.ArgumentNode()
        args.name = _name_node("path")
        args.value = val

        sel.alias = _name_node(f"q{i}")
        sel.arguments = (args,)

        sels.append(sel)

    sel_set = l.SelectionSetNode()
    sel_set.selections = tuple(sels)

    doc = l.parse("""
        query GetNodeType {
            placeholder
        }
        """)

    assert len(doc.definitions) == 1
    query = doc.definitions[0]

    assert isinstance(query, l.OperationDefinitionNode)
    query.selection_set = sel_set

    res = execute(doc)

    acc_info: AccountInfoCurrentPayload = res["accountInfoCurrent"]
    acc_id = acc_info["id"]

    ret: Dict[str, NodeData] = {}
    for i, remote_path in enumerate(remote_paths):
        node: LdataResolvePathToNodePayload = res[f"q{i}"]

        try:
            final_link_target = node["ldataNode"]["finalLinkTarget"]
            remaining = node["path"]

            is_parent = remaining is not None and remaining != ""

            if not allow_resolve_to_parent and is_parent:
                raise ValueError("node does not exist")

            if remaining is not None and "/" in remaining:
                raise ValueError("node and parent does not exist")

            ret[remote_path] = NodeData(
                id=final_link_target["id"],
                name=final_link_target["name"],
                type=LDataNodeType(final_link_target["type"].lower()),
                removed=final_link_target["removed"],
                is_parent=is_parent,
            )
        except (TypeError, ValueError) as e:
            raise FileNotFoundError(get_path_error(remote_path, "not found", acc_id))

    return GetNodeDataResult(acc_id, ret)


@dataclass(frozen=True)
class NodeMetadata:
    id: str
    size: int
    content_type: str


def get_node_metadata(node_id: str) -> NodeMetadata:
    data = execute(
        gql.gql("""
        query NodeMetadataQuery($id: BigInt!) {
            ldataNode(id: $id) {
                removed
                ldataObjectMeta {
                    contentSize
                    contentType
                }
            }
        }
        """),
        variables={"id": node_id},
    )["ldataNode"]
    if data is None or data["removed"]:
        raise FileNotFoundError

    return NodeMetadata(
        id=node_id,
        size=data["ldataObjectMeta"]["contentSize"],
        content_type=data["ldataObjectMeta"]["contentType"],
    )


class PermLevel(str, Enum):
    NONE = "none"
    VIEWER = "viewer"
    MEMBER = "member"
    ADMIN = "admin"
    OWNER = "owner"


@dataclass(frozen=True)
class LDataPerms:
    id: str
    shared: bool
    share_invites: Dict[str, PermLevel]
    share_perms: Dict[AccId, PermLevel]


def get_node_perms(node_id: str) -> LDataPerms:
    data = execute(
        gql.gql("""
        query NodePermissionsQuery($id: BigInt!) {
            ldataNode(id: $id) {
                id
                removed
                ldataSharePermissionsByObjectId {
                    nodes {
                        receiverId
                        level
                    }
                }
                ldataShareInvitesByObjectId {
                    nodes {
                        receiverEmail
                        level
                    }
                }
                shared
            }
        }
        """),
        variables={"id": node_id},
    )["ldataNode"]
    if data is None or data["removed"]:
        raise FileNotFoundError

    return LDataPerms(
        id=node_id,
        shared=data["shared"],
        share_invites={
            node["reveiverEmail"]: node["level"]
            for node in data["ldataShareInvitesByObjectId"]["nodes"]
        },
        share_perms={
            int(node["receiverId"]): node["level"]
            for node in data["ldataSharePermissionsByObjectId"]["nodes"]
        },
    )
