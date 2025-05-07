from typing import Any

from latch.utils import current_workspace
from latch_sdk_config.latch import config

from .interface import session


def launch_workflow(token: str, wf_id: int, params: dict[str, Any]) -> int:
    """Launch the workflow of given id with parameter map.

    Return True if success, raises appropriate exceptions on failure.
    """
    response = session.post(
        config.api.execution.create,
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like"
                " Gecko) Chrome/72.0.3626.119 Safari/537.36"
            ),
        },
        json={
            "workflow_id": str(wf_id),
            "params": params,
            "ws_account_id": current_workspace(),
        },
    )
    response_data = response.json()

    def extract_error_message(data: dict[str, Any]) -> str:
        if "error" in data:
            error = data["error"]
            source = error.get("source", "unknown")

            error_data = error.get("data", {})
            message = (
                error_data.get("stderr") or
                error_data.get("message") or
                str(error_data)
            )

            if isinstance(message, str):
                error_lines = [line for line in message.split("\n") if "Error:" in line]
                if error_lines:
                    message = error_lines[-1].replace("Error:", "").strip()

            return f"({source}): {message}"
        return str(data)

    if response.status_code != 200:
        print("\nRaw server response:")
        print(response_data)

    if response.status_code == 403:
        raise PermissionError(
            "You need access to the latch sdk beta ~ join the waitlist @"
            " https://latch.bio/sdk"
        )
    if response.status_code == 401:
        raise ValueError(
            "your token has expired - please run latch login to refresh your token and"
            " try again."
        )
    if response.status_code == 429:
        error_msg = extract_error_message(response_data)
        print(f"\nFormatted error message: {error_msg}")
        raise RuntimeError(f"Rate limit reached - {error_msg}")
    if response.status_code == 400:
        error_msg = extract_error_message(response_data)
        print(f"\nFormatted error message: {error_msg}")
        raise ValueError(f"Workflow launch failed - {error_msg}")
    if response.status_code != 200:
        error_msg = extract_error_message(response_data)
        print(f"\nFormatted error message: {error_msg}")
        raise RuntimeError(f"Server error (HTTP {response.status_code}) - {error_msg}")
    if "error" in response_data or response_data.get("status") != "Successfully launched workflow":
        error_msg = extract_error_message(response_data)
        print(f"\nFormatted error message: {error_msg}")
        raise RuntimeError(f"Workflow launch failed - {error_msg}")

    execution_id = response_data.get("metadata", {}).get("execution_id")
    if execution_id is None:
        raise RuntimeError("Workflow launch failed - no execution id returned")

    return execution_id
