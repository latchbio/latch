from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # type: ignore  # noqa: PGH003

from latch.utils import current_workspace
from latch_sdk_config.latch import config

session = requests.Session()
retries = 5
retry = Retry(
    total=retries,
    read=retries,
    connect=retries,
    method_whitelist=False,
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


def get_workflow_interface(
    token: str, wf_name: str, version: Optional[str] = None
) -> tuple[int, dict[str, Any], dict[str, Any]]:
    """Retrieves the set of idl parameter values for a given workflow by name.

    Returns workflow id, interface, and default parameters.
    """

    response = session.post(config.api.workflow.interface, headers={"Authorization": f"Bearer {token}"}, json={
        "workflow_name": wf_name,
        "version": version,
        "ws_account_id": current_workspace(),
    })

    wf_interface_resp: dict[str, Any] = response.json()

    wf_id, wf_interface, wf_default_params = (
        wf_interface_resp.get("id"),
        wf_interface_resp.get("interface"),
        wf_interface_resp.get("default_params"),
    )
    if wf_interface is None or wf_id is None or wf_default_params is None:
        message = wf_interface_resp.get("error", {}).get("data", {}).get("message", None)
        if message is None:
            message = f"Could not get interface for workflow {wf_name} {version}"
        raise ValueError(message)

    return int(wf_id), wf_interface, wf_default_params
