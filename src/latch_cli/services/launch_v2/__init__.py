"""Service to launch a workflow."""

import base64
import json
from typing import Any, Optional, Union

import cloudpickle
import google.protobuf.json_format as gpjson
from flyteidl.core import interface_pb2 as _interface_pb2
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.promise import translate_inputs_to_literals
from flytekit.models.interface import Variable, VariableMap

from latch.utils import retrieve_or_login
from latch_cli.services.launch_v2.interface import get_workflow_interface
from latch_cli.services.launch_v2.launch import launch_workflow


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
    token = retrieve_or_login()

    wf_id, interface, _ = get_workflow_interface(token, wf_name, version)

    flyte_interface_types: dict[str, Variable] = VariableMap.from_flyte_idl(gpjson.ParseDict(interface, _interface_pb2.VariableMap())).variables

    python_interface_with_defaults: Union[dict[str, tuple[type, Any]], None] = None
    for v in flyte_interface_types.values():
        description: dict[str, Any] = json.loads(v.description)
        if description.get("idx") != 0:
            continue

        raw_python_interface_with_defaults = description.get("__workflow_meta__", {}).get("meta", {}).get("python_interface")
        if raw_python_interface_with_defaults is not None:
            python_interface_with_defaults = cloudpickle.loads(base64.b64decode(raw_python_interface_with_defaults))
            break

    if python_interface_with_defaults is None:
        raise ValueError("No python interface found -- re-register workflow with latest latch version in workflow Dockerfile")

    for k, v in python_interface_with_defaults.items():
        if k in params:
            continue

        if v[1] is not None:
            params[k] = v[1]
            continue

        t = v[0]
        if hasattr(t, "__origin__") and t.__origin__ is Union and type(None) in t.__args__:
            params[k] = None
        else:
            raise ValueError(f"Required parameter '{k}' not provided in params")

    ctx = FlyteContextManager.current_context()
    assert ctx is not None

    fixed_literals = translate_inputs_to_literals(
        ctx,
        incoming_values=params,
        flyte_interface_types=flyte_interface_types,
        native_types={
            k: v[0] for k, v in python_interface_with_defaults.items()
        },
    )

    return launch_workflow(token, wf_id, {k: gpjson.MessageToDict(v.to_flyte_idl()) for k, v in fixed_literals.items()})
