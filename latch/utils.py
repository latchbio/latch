"""Utility functions for services."""

import jwt
import requests

from latch.config import UserConfig
from latch.services import login


def retrieve_or_login() -> str:
    """Returns a valid JWT to access Latch, prompting a login flow if needed.

    Returns:
        A JWT
    """

    user_conf = UserConfig()
    token = user_conf.token
    if token == "" or not token_is_valid(token):
        token = login()
    return token


def sub_from_jwt(token: str) -> str:
    """Extract a user sub (UUID) from a JWT minted by auth0.

    Args:
        token: JWT

    Returns:
        The user sub contained within the JWT.
    """

    payload = jwt.decode(token, options={"verify_signature": False})
    try:
        sub = payload["sub"]
    except KeyError:
        raise ValueError(
            "Provided token lacks a user sub in the data payload"
            " and is not a valid token."
        )
    return sub


def token_is_valid(token: str) -> bool:
    """Checks if passed token is authenticated with Latch.

    Queries a protected endpoint within the Latch API.

    Args:
        token: JWT

    Returns:
        True if valid.
    """

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        "https://nucleus.latch.bio/api/protected-sdk-ping", headers=headers
    )

    if response.status_code == 200:
        return True
    return False


def account_id_from_token(token: str) -> str:
    """Exchanges a valid JWT for a Latch account ID.

    Latch account IDs are needed for any user-specific request, eg. register
    workflows or copy files to Latch.

    Args:
        token: JWT

    Returns:
        A Latch account ID (UUID).
    """

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        "https://nucleus.latch.bio/sdk/get-account-id", headers=headers
    )

    if response.status_code != 200:
        raise Exception(
            f"Could not retrieve id from token - {token}.\n\t{response.text}"
        )
    return response.json()["id"]
