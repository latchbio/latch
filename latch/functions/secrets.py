import os

from latch_cli.config.latch import config
from latch_cli.tinyrequests import post
from latch_cli.utils import current_workspace, retrieve_or_login


def get_secret(secret_name: str):
    """
    A utility to allow users to reference secrets stored in their workspace on
    Latch.

    Important: When running an execution locally, whether on your own computer
    or using `latch develop`, the only secrets you will be able to access are
    the ones in your personal workspace. To use secrets from a shared workspace,
    register your workflow and run it on Latch.

    Examples:
        >>> get_secret("test-secret")
        "test-value-123"
    """
    execution_token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if execution_token is None:
        return _get_secret_local(secret_name)

    resp = post(
        url=config.api.user.get_secret,
        json={
            "execution_token": execution_token,
            "name": secret_name,
        },
    )

    if resp.status_code != 200:
        raise ValueError(resp.json()["error"])

    return resp.json()["secret"]


def _get_secret_local(secret_name: str):
    resp = post(
        url=config.api.user.get_secret_local,
        json={
            "ws_account_id": current_workspace(),
            "name": secret_name,
        },
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
    )

    if resp.status_code != 200:
        raise ValueError(resp.json()["error"])

    return resp.json()["secret"]
