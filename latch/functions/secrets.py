import os

from latch_cli.config.latch import config
from latch_cli.tinyrequests import post
from latch_cli.utils import current_workspace, retrieve_or_login


def get_secret(secret_name: str):
    """
    A utility to allow users to reference secrets stored in their workspace on
    Latch.

    Examples:
        >>> get_secret("test-secret")
        "test-value-123"
    """
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
        raise ValueError(resp.json()["error"])

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
        raise ValueError(resp.json()["error"])

    return resp.json()["secret"]
