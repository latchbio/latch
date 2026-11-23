from urllib.parse import urljoin

from latch.utils import current_workspace
from latch_cli.tinyrequests import post
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import NUCLEUS_URL


def get_secret(secret_name: str):
    """Reference a secret stored in a workspace on Latch.

    Important: When running an execution locally on your own computer, the only
    secrets you will be able to access are the ones in your personal workspace.
    To use secrets from a shared workspace, register your workflow and run it
    on Latch.

    Examples:
        >>> get_secret("test-secret")
        "test-value-123"
    """

    resp = post(
        url=urljoin(NUCLEUS_URL, "/secrets/get-new"),
        json={"name": secret_name, "ws_id": current_workspace()},
        headers={"Authorization": get_auth_header()},
    )

    if resp.status_code != 200:
        raise ValueError(resp.json()["error"])

    return resp.json()["secret"]
