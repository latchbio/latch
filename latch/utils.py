import itertools
import os
from typing import Dict, TypedDict

import gql
import jwt
from latch_sdk_config.user import user_config
from latch_sdk_gql.execute import execute


def account_id_from_token(token: str) -> str:
    """Exchanges a valid JWT for a Latch account ID.

    Latch account IDs are needed for any user-specific request, eg. register
    workflows or copy files to Latch.

    Args:
        token: JWT

    Returns:
        A Latch account ID (UUID).
    """
    decoded_jwt = jwt.decode(token, options={"verify_signature": False})
    try:
        return decoded_jwt.get("id")
    except KeyError as e:
        raise ValueError("Your Latch access token is malformed") from e


def retrieve_or_login() -> str:
    """Returns a valid JWT to access Latch, prompting a login flow if needed.

    Returns:
        A JWT
    """
    from latch_cli.services.login import login

    token = user_config.token
    if token == "":
        token = login()
    return token


class WSInfo(TypedDict):
    workspace_id: str
    name: str
    default: bool


def get_workspaces() -> Dict[str, WSInfo]:
    """Retrieve workspaces that user can access.

    Returns:
        A dictionary mapping workspace IDs to workspace display names.
    """
    account_id = account_id_from_token(retrieve_or_login())
    res = execute(
        gql.gql("""
            query GetWorkspaces($accountId: BigInt!) {
                userInfoByAccountId(accountId: $accountId) {
                    id
                    defaultAccount
                }
                teamInfoByAccountId(accountId: $accountId) {
                    accountId
                    displayName
                }
                teamInfos(filter: {owner: {accountId: {equalTo: $accountId}}}) {
                    nodes {
                        accountId
                        displayName
                    }
                }
                teamMembers(filter: {user: {accountId: {equalTo: $accountId}}}) {
                    nodes {
                        team {
                            accountId
                            displayName
                        }
                    }
                }
                orgInfos(filter: { ownerAccountId: { equalTo: $accountId } }) {
                    nodes {
                        teamInfosByOrgId(filter: { account: { removed: { equalTo: false } } }) {
                            nodes {
                                accountId
                                displayName
                            }
                        }
                    }
                }
                orgMembers(filter: { userAccountId: { equalTo: $accountId } }) {
                    nodes {
                        org {
                            teamInfosByOrgId(filter: { account: { removed: { equalTo: false } } }) {
                                nodes {
                                    accountId
                                    displayName
                                }
                            }
                        }
                    }
                }
            }
        """),
        {"accountId": account_id},
    )

    owned_teams = res["teamInfos"]["nodes"]
    member_teams = [x["team"] for x in res["teamMembers"]["nodes"]]

    owned_org_teams = [x["teamInfosByOrgId"]["nodes"] for x in res["orgInfos"]["nodes"]]
    owned_org_teams = list(itertools.chain(*owned_org_teams))

    member_org_teams = [
        x["org"]["teamInfosByOrgId"]["nodes"] for x in res["orgMembers"]["nodes"]
    ]
    member_org_teams = list(itertools.chain(*member_org_teams))

    default_account = (
        res["userInfoByAccountId"]["defaultAccount"]
        if res["userInfoByAccountId"] is not None
        else None
    )
    teams = {
        x["accountId"]: WSInfo(
            workspace_id=x["accountId"],
            name=x["displayName"],
            default=x["accountId"] == default_account,
        )
        for x in owned_teams
        + member_teams
        + (
            [res["teamInfoByAccountId"]]
            if res["teamInfoByAccountId"] is not None
            else []
        )
        + owned_org_teams
        + member_org_teams
    }

    return teams


def current_workspace() -> str:
    """Retrieves the current workspace ID based on the user's configuration.

    If the workspace ID is not set, it retrieves the default workspace ID from the user's account.
    If the default workspace ID is not set, it raises a ValueError.
    """
    ws = user_config.workspace_id
    if ws == "":
        res = execute(
            gql.gql("""
                query DefaultAccountQuery {
                    accountInfoCurrent {
                        id
                        user {
                            defaultAccount
                        }
                    }
                }
            """),
        )["accountInfoCurrent"]

        default_ws = res["id"]

        is_local = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID") is None
        if is_local and res["user"] is not None:
            default_ws = res["user"]["defaultAccount"]

        if default_ws is not None:
            ws = default_ws

    return ws


class NotFoundError(ValueError): ...
