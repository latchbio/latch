from dataclasses import dataclass
from typing import Dict, List, TypedDict

import graphql.language as l
from latch_sdk_gql.utils import _name_node, _parse_selection
from typing_extensions import TypeAlias

from latch.ldata.type import LatchPathError, LDataNodeType
from latch_cli.utils.path import normalize_path

from .utils import query_with_retry

AccId: TypeAlias = int


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
    remaining: str

    def is_direct_parent(self) -> bool:
        return self.remaining is not None and "/" not in self.remaining

    def exists(self) -> bool:
        return self.remaining is None or self.remaining == ""


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

    res = query_with_retry(doc)

    acc_info: AccountInfoCurrentPayload = res["accountInfoCurrent"]
    acc_id = acc_info["id"]

    ret: Dict[str, NodeData] = {}
    for i, remote_path in enumerate(remote_paths):
        node: LdataResolvePathToNodePayload = res[f"q{i}"]

        try:
            remaining = node["path"]
            if (
                remaining is not None and remaining != ""
            ) and not allow_resolve_to_parent:
                raise LatchPathError(
                    f"no such Latch file or directory", remote_path, acc_id
                )

            final_link_target = node["ldataNode"]["finalLinkTarget"]
            ret[remote_path] = NodeData(
                id=final_link_target["id"],
                name=final_link_target["name"],
                type=LDataNodeType(final_link_target["type"].lower()),
                remaining=remaining,
            )
        except (TypeError, ValueError):
            raise LatchPathError(
                f"no such Latch file or directory", remote_path, acc_id
            )

    return GetNodeDataResult(acc_id, ret)
