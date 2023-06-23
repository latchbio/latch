"""Service to get workflows. """

from typing import List, Optional, Tuple, Union

import requests
from latch_sdk_config.latch import config

from latch_cli.utils import current_workspace, retrieve_or_login


# TODO(ayush): rewrite this to look/be better
def get_wf(wf_name: Optional[str] = None):
    """Get a list of a workflow's versions.

    This will allow users to list all owned workflows by default. Optionally, a
    user can provide a workflow name (unique with respect to a user) to
    list all versions of the specific workflow.

    Args:
        wf_name: The name of the workflow.

    Example:
        >>> get_wf("wf.__init__.alphafold_wf")
            ID      Name                            Version
            61858   wf.__init__.alphafold_wf        v2.1.0+14
            67261   wf.__init__.alphafold_wf        v2.2.3+0
            67317   wf.__init__.alphafold_wf        v2.2.3+14
            67341   wf.__init__.alphafold_wf        v2.2.3+19
            67408   wf.__init__.alphafold_wf        v2.2.3+40
            ...
    """

    token = retrieve_or_login()
    return _list_workflow_request(token, wf_name)


def _list_workflow_request(
    token: str, wf_name: Union[None, str]
) -> List[List[Tuple[str, str, str]]]:
    """Fetch a list of workflows, potentially by name."""

    headers = {
        "Authorization": f"Bearer {token}",
    }
    data = {
        "workflow_name": wf_name,
        "ws_account_id": current_workspace(),
    }

    url = config.api.workflow.list
    response = requests.post(url, headers=headers, json=data)

    wf_interface_resp = response.json()
    return wf_interface_resp.get("wfs", [])
