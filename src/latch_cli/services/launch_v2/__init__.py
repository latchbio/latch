"""Service to launch a workflow."""

from typing import Any, Optional, Union, Callable

import google.protobuf.json_format as gpjson
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.promise import translate_inputs_to_literals
from flytekit.core.workflow import PythonFunctionWorkflow

from latch.utils import retrieve_or_login
from latch_cli.services.launch_v2.interface import get_workflow_interface
from latch_cli.services.launch_v2.launch import launch_workflow


def launch(*, workflow: Callable[..., Any], wf_name: str, params: dict[str, Any], version: Optional[str] = None) -> int:
    """Launch the workflow defined by the function signature with the parameters specified in params.

    If version is provided, the specified version of the workflow will be launched. Launching old versions of the workflow
    may fail if the workflow interface has changed.

    Args:
        workflow: The workflow to launch.
        wf_name: The name of the workflow to launch.
        params: A dictionary of parameters to pass to the workflow.
        version: An optional workflow version to launch, defaulting to latest.

    Returns:
        Execution ID of the launched workflow.
    """

    if not isinstance(workflow, PythonFunctionWorkflow):
        raise TypeError("Workflow must be a PythonFunctionWorkflow")

    token = retrieve_or_login()

    for key, t in workflow.python_interface.inputs.items():
        if key not in params and key not in workflow.python_interface.default_inputs_as_kwargs:
            if hasattr(t, "__origin__") and t.__origin__ is Union and type(None) in t.__args__:
                params[key] = None
            else:
                raise ValueError(f"Required parameter '{key}' not provided in params")

    ctx = FlyteContextManager.current_context()
    assert ctx is not None

    fixed_literals = translate_inputs_to_literals(
        ctx,
        incoming_values=params,
        flyte_interface_types=workflow.interface.inputs,
        native_types=workflow.python_interface.inputs,
    )

    wf_id, _, _ = get_workflow_interface(token, wf_name, version)

    return launch_workflow(token, wf_id, {k: gpjson.MessageToDict(v.to_flyte_idl()) for k, v in fixed_literals.items()})
