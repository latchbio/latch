from json.decoder import JSONDecodeError
from typing import Any, Optional

from latch_cli import tinyrequests
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import config


def get_workflow_interface(
    target_account_id: str, wf_name: str, version: Optional[str] = None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Retrieves the set of idl parameter values for a given workflow by name.

    Returns workflow id, interface, and default parameters.
    """

    response = tinyrequests.post(config.api.workflow.interface, headers={"Authorization": get_auth_header()}, json={
        "workflow_name": wf_name,
        "version": version,
        "ws_account_id": target_account_id,
    })

    try:
        wf_interface_resp: dict[str, Any] = response.json()
    except JSONDecodeError as e:
        raise RuntimeError(f"Could not parse response as JSON: ({response.status_code}) {response}") from e

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

    return str(wf_id), wf_interface, wf_default_params
