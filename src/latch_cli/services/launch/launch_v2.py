import base64
import json
from json.decoder import JSONDecodeError
from typing import Any, Optional, Union, get_args, get_origin

import dill
import google.protobuf.json_format as gpjson
from flyteidl.core import interface_pb2 as _interface_pb2
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.promise import translate_inputs_to_literals
from flytekit.core.type_engine import TypeTransformerFailedError
from flytekit.models.interface import Variable, VariableMap

from latch.utils import current_workspace
from latch_cli import tinyrequests
from latch_cli.services.launch.interface import get_workflow_interface
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import config


def launch(*, wf_name: str, params: dict[str, Any], version: Optional[str] = None) -> int:
    """Create an execution of workflow `wf_name` with version `version` and parameters `params`.

    If version is not provided, the latest version of the workflow will be launched.

    This command is not backwards compatible with workflows registered with latch version < 2.62.0 in the container.

    Args:
        wf_name: Name of workflow to launch (see `.latch/workflow_name` in the workflow directory).
        params: A dictionary of parameters to pass to the workflow.
        version: An optional workflow version to launch, defaulting to latest.

    Returns:
        Execution ID of the launched workflow.
    """
    target_account_id = current_workspace()

    wf_id, interface, _ = get_workflow_interface(target_account_id, wf_name, version)

    flyte_interface_types: dict[str, Variable] = VariableMap.from_flyte_idl(gpjson.ParseDict(interface, _interface_pb2.VariableMap())).variables

    python_interface_with_defaults: Union[dict[str, tuple[type, Any]], None] = None
    for v in flyte_interface_types.values():
        description: dict[str, Any] = json.loads(v.description)
        if description.get("idx") != 0:
            continue

        raw_python_interface_with_defaults = description.get("__workflow_meta__", {}).get("meta", {}).get("python_interface")
        if raw_python_interface_with_defaults is not None:
            try:
                python_interface_with_defaults = dill.loads(base64.b64decode(raw_python_interface_with_defaults))  # noqa: S301
            except dill.UnpicklingError as e:
                raise ValueError("Failed to decode the workflow python interface -- ensure your python version matches the version in the workflow environment") from e
            break

    if python_interface_with_defaults is None:
        raise ValueError("No python interface found -- re-register workflow with latch version >= 2.62.0 in workflow environment")

    for k, v in python_interface_with_defaults.items():
        if k in params:
            continue

        if v[1] is not None:
            params[k] = v[1]
            continue

        t = v[0]
        if get_origin(t) is Union and type(None) in get_args(t):
            params[k] = None
        else:
            raise ValueError(f"Required parameter '{k}' not provided in params")

    ctx = FlyteContextManager.current_context()
    assert ctx is not None

    try:
        fixed_literals = translate_inputs_to_literals(
            ctx,
            incoming_values=params,
            flyte_interface_types=flyte_interface_types,
            native_types={
                k: v[0] for k, v in python_interface_with_defaults.items()
            },
        )
    except TypeTransformerFailedError as e:
        if "is not an instance of" in str(e):
            raise ValueError("Failed to translate inputs to literals -- ensure you are importing the same classes used in the workflow function header") from e
        raise

    return launch_workflow(target_account_id, wf_id, {k: gpjson.MessageToDict(v.to_flyte_idl()) for k, v in fixed_literals.items()})


def launch_workflow(target_account_id: str, wf_id: str, params: dict[str, Any]) -> int:
    """Launch the workflow of given id with parameter map.

    Return True if success, raises appropriate exceptions on failure.
    """
    response = tinyrequests.post(
        config.api.execution.create,
        headers={
            "Authorization": get_auth_header(),
            "User-Agent": (  # this is from a 3 yr old comment which says this was needed or the endpoint would hang sometimes?
                "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like"
                " Gecko) Chrome/72.0.3626.119 Safari/537.36"
            ),
        },
        json={
            "workflow_id": wf_id,
            "params": params,
            "ws_account_id": target_account_id,
        },
    )

    try:
        response_data = response.json()
    except JSONDecodeError as e:
        raise RuntimeError(f"Could not parse response as JSON: ({response.status_code}) {response}") from e

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
