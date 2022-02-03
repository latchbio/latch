"""
utils
~~~~~
Hammers and nails.
"""

import jwt
import requests

from latch.config import UserConfig
from latch.services.login import login


def sub_from_jwt(token: str) -> str:
    """Extract a user sub from a generic jwt."""

    payload = jwt.decode(token, options={"verify_signature": False})
    try:
        sub = payload["sub"]
    except KeyError:
        raise ValueError(
            "Provided token lacks a user sub in the data payload"
            " and is not a valid token."
        )
    return sub


def retrieve_or_login() -> str:
    """Returns a valid token, prompting a login flow if needed."""

    user_conf = UserConfig()
    token = user_conf.token
    if token == "" or not token_is_valid(token):
        token = login()
    return token


def token_is_valid(token: str) -> bool:

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        "https://nucleus.latch.bio/api/protected-sdk-ping", headers=headers
    )

    if response.status_code == 200:
        return True
    return False


def account_id_from_token(token: str) -> str:

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        "https://nucleus.latch.bio/sdk/get-account-id", headers=headers
    )

    if response.status_code != 200:
        raise Exception(
            f"Could not retrieve id from token - {token}.\n\t{response.text}"
        )
    return response.json()["id"]
