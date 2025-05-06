"""Service to launch a workflow."""

from typing import Any, Optional, Union

import google.protobuf.json_format as gpjson
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.interface import Interface, transform_inputs_to_parameters
from flytekit.core.promise import translate_inputs_to_literals
from flytekit.core.workflow import PythonFunctionWorkflow

from latch.utils import retrieve_or_login
from latch_cli.services.launch_v2.interface import get_workflow_interface
from latch_cli.services.launch_v2.launch import launch_workflow


def launch(*, workflow: PythonFunctionWorkflow, wf_name: str, params: dict[str, Any], version: Optional[str] = None) -> int:
    """Launches a (versioned) workflow with parameters specified in python.

    Using a parameter map written in python (this can be generated for you with
    `get_params`), this function will launch the workflow specified in the file
    using the parameters therein. This function also accepts an optional
    `version` parameter to further specify the workflow to be run. If it is not
    provided, this function will default to running the latest version of the
    specified workflow.

    Args:
        workflow: The workflow to launch.
        wf_name: The name of the workflow to launch.
        params: A dictionary of parameters to pass to the workflow.
        version: An optional workflow version to launch, defaulting to latest.

    Returns:
        Execution ID of the launched workflow.
    """

    token = retrieve_or_login()

    ctx = FlyteContextManager.current_context()
    if ctx is None:
        raise ValueError("No context found")

    for key, t in workflow.python_interface.inputs.items():
        if key not in params and key not in workflow.python_interface.default_inputs_as_kwargs:
            if hasattr(t, "__origin__") and t.__origin__ is Union and type(None) in t.__args__:
                params[key] = None
            else:
                raise ValueError(f"Required parameter '{key}' not provided in params")

    fixed_literals = translate_inputs_to_literals(
        ctx,
        incoming_values=params,
        flyte_interface_types=workflow.interface.inputs,
        native_types=workflow.python_interface.inputs,
    )

    wf_id, _, _ = get_workflow_interface(token, wf_name, version)

    return launch_workflow(token, wf_id, {k: gpjson.MessageToDict(v.to_flyte_idl()) for k, v in fixed_literals.items()})
