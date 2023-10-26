from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, TypedDict

import click
import dateutil.parser as dp

from latch_cli.utils import urljoins

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import gql
import graphql.language as l
from latch_sdk_gql.execute import execute
from latch_sdk_gql.utils import _name_node, _parse_selection

from latch_cli.utils.path import get_path_error, normalize_path


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
    ldataObjectMeta: Optional["LdataObjectMeta"]


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
    modify_time: Optional[datetime]


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
                        ldataObjectMeta {
                            modifyTime
                        }
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
            lom = final_link_target["ldataObjectMeta"]
            remaining = node["path"]

            is_parent = remaining is not None and remaining != ""

            if not allow_resolve_to_parent and is_parent:
                raise ValueError("node does not exist")

            if remaining is not None and "/" in remaining:
                raise ValueError("node and parent does not exist")

            modify_time: Optional[datetime] = None
            if lom is not None and lom["modifyTime"] is not None:
                modify_time = dp.isoparse(lom["modifyTime"])

            ret[remote_path] = NodeData(
                id=final_link_target["id"],
                name=final_link_target["name"],
                type=LDataNodeType(final_link_target["type"].lower()),
                is_parent=is_parent,
                modify_time=modify_time,
            )
        except (TypeError, ValueError) as e:
            click.echo(get_path_error(remote_path, "not found", acc_id))
            raise click.exceptions.Exit(1) from e

    return GetNodeDataResult(acc_id, ret)


class Child(TypedDict):
    name: str


class ChildLdataTreeEdgesNode(TypedDict):
    child: Child


class ChildLdataTreeEdges(TypedDict):
    nodes: List[ChildLdataTreeEdgesNode]


class LdataResolvePathData(TypedDict):
    childLdataTreeEdges: ChildLdataTreeEdges


@cache
def _get_immediate_children_of_node(path: str) -> List[str]:
    lrpd: LdataResolvePathData = execute(
        gql.gql("""
            query MyQuery($argPath: String!) {
                ldataResolvePathData(argPath: $argPath) {
                    childLdataTreeEdges(
                        filter: {child: {removed: {equalTo: false}}}
                    ) {
                        nodes {
                            child {
                                name
                            }
                        }
                    }
                }
            }
        """),
        {"argPath": path},
    )["ldataResolvePathData"]

    res: List[str] = []
    for node in lrpd["childLdataTreeEdges"]["nodes"]:
        res.append(node["child"]["name"])

    return res


class Team(TypedDict):
    accountId: str


class TeamMembersByUserIdNode(TypedDict):
    team: Team


class TeamMembersByUserId(TypedDict):
    nodes: List[TeamMembersByUserIdNode]


class TeamInfosByOwnerId(TypedDict):
    nodes: List[Team]


class UserInfoByAccountId(TypedDict):
    defaultAccount: str
    teamMembersByUserId: TeamMembersByUserId
    teamInfosByOwnerId: TeamInfosByOwnerId


class Bucket(TypedDict):
    bucketName: str


class LdataS3MountAccessProvensByGeneratedUsing(TypedDict):
    nodes: List[Bucket]


class LdataS3MountConfiguratorRolesByAccountIdNode(TypedDict):
    ldataS3MountAccessProvensByGeneratedUsing: LdataS3MountAccessProvensByGeneratedUsing


class LdataS3MountConfiguratorRolesByAccountId(TypedDict):
    nodes: List[LdataS3MountConfiguratorRolesByAccountIdNode]


class AccountInfoCurrent(TypedDict):
    userInfoByAccountId: UserInfoByAccountId
    ldataS3MountConfiguratorRolesByAccountId: LdataS3MountConfiguratorRolesByAccountId


@cache
def _get_known_domains_for_account() -> List[str]:
    aic: AccountInfoCurrent = execute(gql.gql("""
        query DomainCompletionQuery {
            accountInfoCurrent {
                userInfoByAccountId {
                    defaultAccount
                    teamMembersByUserId(
                        filter: { team: { account: { removed: { equalTo: false } } } }
                    ) {
                        nodes {
                            team {
                                accountId
                            }
                        }
                    }
                    teamInfosByOwnerId(filter: { account: { removed: { equalTo: false } } }) {
                        nodes {
                            accountId
                        }
                    }
                }
                ldataS3MountConfiguratorRolesByAccountId {
                    nodes {
                        ldataS3MountAccessProvensByGeneratedUsing {
                            nodes {
                                bucketName
                            }
                        }
                    }
                }
            }
        }
    """))["accountInfoCurrent"]

    ui = aic["userInfoByAccountId"]

    res: List[str] = [""]  # "" is for latch:///

    accs: List[int] = [int(ui["defaultAccount"])]
    accs.extend(
        int(tm["team"]["accountId"]) for tm in ui["teamMembersByUserId"]["nodes"]
    )
    accs.extend(int(ti["accountId"]) for ti in ui["teamInfosByOwnerId"]["nodes"])
    accs.sort()
    for x in accs:
        res.append(f"{x}.account")
        res.append(f"shared.{x}.account")

    buckets = [
        map["bucketName"]
        for role in aic["ldataS3MountConfiguratorRolesByAccountId"]["nodes"]
        for map in role["ldataS3MountAccessProvensByGeneratedUsing"]["nodes"]
    ]
    buckets.sort()
    res.extend(f"{x}.mount" for x in buckets)

    return res


class LdataObjectMeta(TypedDict):
    modifyTime: Optional[str]


class InnerFinalLinkTarget(TypedDict):
    id: str
    ldataObjectMeta: Optional[LdataObjectMeta]


class NodeDescendantData(TypedDict):
    finalLinkTarget: InnerFinalLinkTarget


class Node(TypedDict):
    relPath: str
    node: NodeDescendantData


class Descendants(TypedDict):
    nodes: List[Node]


class OuterFinalLinkTarget(TypedDict):
    descendants: Descendants


class NodeDescendantsLdataResolvePathData(TypedDict):
    finalLinkTarget: OuterFinalLinkTarget


@dataclass(frozen=True)
class NodeDescendant:
    id: str
    modify_time: Optional[datetime]


@dataclass(frozen=True)
class GetNodeDescendantsResult:
    nodes: Dict[str, NodeDescendant]


@cache
def get_node_descendants(path: str) -> GetNodeDescendantsResult:
    res: Dict[str, NodeDescendant] = {}
    data: Optional[NodeDescendantsLdataResolvePathData] = execute(
        gql.gql("""
            query NodeDescendants($argPath: String!) {
                ldataResolvePathData(argPath: $argPath) {
                    finalLinkTarget {
                        descendants {
                            nodes {
                                relPath
                                node {
                                    finalLinkTarget {
                                        id
                                        ldataObjectMeta {
                                            modifyTime
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """),
        {"argPath": path},
    )["ldataResolvePathData"]

    if data is None:
        return GetNodeDescendantsResult(nodes=res)

    for descendant in data["finalLinkTarget"]["descendants"]["nodes"]:
        rel_path = descendant["relPath"]
        flt = descendant["node"]["finalLinkTarget"]
        lom = flt["ldataObjectMeta"]

        modify_time: Optional[datetime] = None
        if lom is not None and lom["modifyTime"] is not None:
            modify_time = dp.isoparse(lom["modifyTime"])

        res[rel_path] = NodeDescendant(id=flt["id"], modify_time=modify_time)

    return GetNodeDescendantsResult(nodes=res)
