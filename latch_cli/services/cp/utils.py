from typing import List, TypedDict

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import gql
from latch_sdk_gql.execute import execute


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

    if lrpd is None:
        return []

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


# todo(taras): support for gcp and azure mounts
# skipping now due to time. This decision does not
# influence correcetness of the CLI and only
# reduces the set of returned autocomplete
# suggestions
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
