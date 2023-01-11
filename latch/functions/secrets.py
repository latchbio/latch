import os

from latch_cli.config.latch import _LatchConfig
from latch_cli.tinyrequests import post
from latch_cli.utils import current_workspace, retrieve_or_login

config = _LatchConfig()
endpoints = config.sdk_endpoints


def get_secret(secret_name: str):
    internal_execution_id = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if internal_execution_id is None:
        return _get_secret_local(secret_name)

    resp = post(
        url=endpoints["get-secret"],
        json={
            "internal_execution_id": internal_execution_id,
            "name": secret_name,
        },
    )

    if resp.status_code != 200:
        raise ValueError(resp.json()["error"]["data"]["error"])

    return resp.json()["secret"]


def _get_secret_local(secret_name: str):
    resp = post(
        url=endpoints["get-secret-local"],
        json={
            "ws_account_id": current_workspace(),
            "name": secret_name,
        },
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
    )

    if resp.status_code != 200:
        raise ValueError(resp.json()["error"]["data"]["error"])

    return resp.json()["secret"]
