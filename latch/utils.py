import os
from typing import Dict

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


def get_workspaces() -> Dict[str, str]:
    """Retrieve workspaces that user can access.

    Returns:
        A dictionary mapping workspace IDs to workspace display names.
    """
    account_id = account_id_from_token(retrieve_or_login())
    res = execute(
        gql.gql("""
        query GetUserDefaultWorkspace($accountId: BigInt!) {
            teamInfos(filter: {owner: {accountId: {equalTo: $accountId}}}) {
                nodes {
                    displayName
                    accountId
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
        }"""),
        {"accountId": account_id},
    )

    owned_teams = res["teamInfos"]["nodes"]
    member_teams = [x["team"] for x in res["teamMembers"]["nodes"]]

    teams = {x["accountId"]: x["displayName"] for x in owned_teams + member_teams}
    teams[account_id] = "Personal Workspace"

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
        if is_local:
            default_ws = res["user"]["defaultAccount"]

        if default_ws is not None:
            ws = default_ws

    return ws
