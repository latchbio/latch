from urllib.parse import urljoin

from latch_sdk_config.latch import NUCLEUS_URL

from latch.utils import current_workspace
from latch_cli.tinyrequests import post
from latch_cli.utils import get_auth_header


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

    resp = post(
        url=urljoin(NUCLEUS_URL, "/secrets/get-new"),
        json={
            "name": secret_name,
            "ws_id": current_workspace(),
        },
        headers={"Authorization": get_auth_header()},
    )

    if resp.status_code != 200:
        raise ValueError(resp.json()["error"])

    return resp.json()["secret"]
