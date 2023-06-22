try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import os
import re
from pathlib import Path
from typing import List, TypedDict

import click
import click.shell_completion as sc
import gql
from latch_sdk_gql.execute import execute

from latch_cli.services.cp.path_utils import urljoins

completion_type = re.compile(
    r"""
    ^(latch)? ://(
        (?P<domain>[^/]*)
        | (?P<remote_path>[^/]*/.*)
    )$
    """,
    re.VERBOSE,
)


def complete(
    ctx: click.Context,
    param: click.Argument,
    incomplete: str,
) -> List[sc.CompletionItem]:
    match = completion_type.match(incomplete)

    if match is None:
        return _complete_local_path(incomplete)
    elif match["domain"] is not None:
        return _complete_domain(incomplete)
    else:
        return _complete_remote_path(incomplete)


def remote_complete(
    ctx: click.Context,
    param: click.Argument,
    incomplete: str,
):
    match = completion_type.match(incomplete)

    if match is None:
        return []
    elif match["domain"]:
        return _complete_domain(incomplete)
    else:
        return _complete_remote_path(incomplete)


@cache
def _complete_local_path(incomplete: str) -> List[sc.CompletionItem]:
    if incomplete == "":
        parent = Path.cwd()
        stub = ""
    else:
        p = Path(incomplete).resolve()
        parent = p.parent
        stub = p.name

    res: List[sc.CompletionItem] = []
    for sub_path in parent.iterdir():
        if not sub_path.name.startswith(stub):
            continue

        rel_path = os.path.relpath(sub_path)
        typ = "file" if sub_path.is_file() else "dir"
        res.append(sc.CompletionItem(rel_path, type=typ))

    return res


# `incomplete` assumed to be of the form '(latch)?://[DOMAIN]/.*'
@cache
def _complete_remote_path(incomplete: str) -> List[sc.CompletionItem]:
    parent, stub = tuple(incomplete.rsplit("/", 1))
    children = _get_immediate_children_of_node(parent)

    res: List[sc.CompletionItem] = []
    for child in children:
        if child.startswith(stub):
            res.append(sc.CompletionItem(urljoins(parent, child)))

    return res


domain = re.compile(r"^(latch)?://(?P<stub>[^/]*)$")


# `incomplete` assumed to be of the form '(latch)?://[^/]*'
@cache
def _complete_domain(incomplete: str) -> List[sc.CompletionItem]:
    match = domain.match(incomplete)
    if match is None:
        return []

    stub = match["stub"]

    res: List[sc.CompletionItem] = []
    for d in _get_all_valid_domains_for_account():
        if d.startswith(stub):
            res.append(sc.CompletionItem(f"latch://{d}/"))

    return res


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
def _get_all_valid_domains_for_account() -> List[str]:
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

    res: List[str] = [
        f"{ui['defaultAccount']}.account",
        f"shared.{ui['defaultAccount']}.account",
    ]
    for node in ui["teamMembersByUserId"]["nodes"]:
        account_id = node["team"]["accountId"]
        res.append(f"{account_id}.account")
        res.append(f"shared.{account_id}.account")
    for node in ui["teamInfosByOwnerId"]["nodes"]:
        account_id = node["accountId"]
        res.append(f"{account_id}.account")
        res.append(f"shared.{account_id}.account")

    s3_roles = aic["ldataS3MountConfiguratorRolesByAccountId"]
    for node in s3_roles["nodes"]:
        for sub_node in node["ldataS3MountAccessProvensByGeneratedUsing"]["nodes"]:
            res.append(f"{sub_node['bucketName']}.mount")

    return res
