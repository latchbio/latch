from dataclasses import dataclass
from typing import Dict, List, TypedDict

import graphql.language as l
from latch_sdk_gql.execute import execute
from latch_sdk_gql.utils import _name_node, _parse_selection
from typing_extensions import TypeAlias

from latch.ldata.type import LDataNodeType
from latch_cli.utils.path import get_path_error, normalize_path

AccId: TypeAlias = int


class LatchPathNotFound(RuntimeError):
    pass


class LDataObjectMeta(TypedDict):
    contentSize: str
    contentType: str


class FinalLinkTargetPayload(TypedDict):
    id: str
    type: str
    name: str
    removed: bool
    ldataObjectMeta: LDataObjectMeta


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
                is_parent=is_parent,
            )
        except (TypeError, ValueError) as e:
            raise LatchPathNotFound(get_path_error(remote_path, "not found", acc_id))

    return GetNodeDataResult(acc_id, ret)
