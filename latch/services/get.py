"""Service to get workflows. """

from typing import List, Tuple, Union

import requests
from latch.utils import retrieve_or_login


def get_wf(wf_name: Union[None, str] = None, wf_id: Union[None, str] = None):
    """Get a list of workflows a versions.

    Args:
        wf_name: The unique name of a workflow.
        wf_id: The unique ID of a workflow.

    This will allow users to list all owned workflows by default. Optionally, a
    user can provide a workflow id or a workflow name (both unique with respect
    to a user) to list all versions associated with a workflow.

    The subcommand naming and behavior is inspired by `kubectl get`.

    Example: ::

        get_wf()
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
    _request = {"workflow_name": wf_name}

    url = "https://nucleus.latch.bio/sdk/get-wf"

    response = requests.post(url, headers=headers, json=_request)
    if response.status_code == 403:
        raise PermissionError(
            "You need access to the Latch SDK beta ~ join the waitlist @ https://latch.bio/sdk"
        )

    wf_interface_resp = response.json()
    return wf_interface_resp.get("wfs", [])
