"""Service to launch a workflow."""

from typing import Any, Optional

import google.protobuf.json_format as gpjson
from flyteidl.core import interface_pb2 as _interface_pb2
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.promise import translate_inputs_to_literals
from flytekit.models import literals as _literals
from flytekit.models.interface import VariableMap

from latch.utils import retrieve_or_login
from latch_cli.services.launch_v2.interface import get_workflow_interface
from latch_cli.services.launch_v2.launch import launch_workflow


def launch(*, wf_name: str, params: dict[str, Any], version: Optional[str] = None) -> str:
    """Launches a (versioned) workflow with parameters specified in python.

    Using a parameter map written in python (this can be generated for you with
    `get_params`), this function will launch the workflow specified in the file
    using the parameters therein. This function also accepts an optional
    `version` parameter to further specify the workflow to be run. If it is not
    provided, this function will default to running the latest version of the
    specified workflow.

    Args:
        wf_name: The name of the workflow to launch.
        params: A dictionary of parameters to pass to the workflow.
        version: An optional workflow version to launch, defaulting to latest.

    Returns:
        Execution ID of the launched workflow.

    Example:
        >>> launch(wf_name="wf.__init__.assemble_and_sort", params_dict={"x": 1, "y": 2})
            # Launches an execution of `wf.__init__.assemble_and_sort` with the
            # parameters specified in the referenced file.
    """

    token = retrieve_or_login()

    wf_id, wf_interface, wf_default_params = get_workflow_interface(token, wf_name, version)

    ctx = FlyteContextManager.current_context()
    if ctx is None:
        raise ValueError("No context found")

    wf_interface_pb2 = gpjson.ParseDict(wf_interface, _interface_pb2.VariableMap())
    flyte_interface_types = VariableMap.from_flyte_idl(wf_interface_pb2)

    literals = translate_inputs_to_literals(
        ctx,
        incoming_values=params,
        flyte_interface_types=flyte_interface_types.variables,
        native_types={
            k: type(v) for k, v in params.items()
        },
    )

    param_defaults: dict[str, Any] = wf_default_params["parameters"]

    for key, default_value in param_defaults.items():
        if key not in literals:
            default = default_value.get("default")
            if default is not None:
                literals[key] = _literals.Literal.from_flyte_idl(gpjson.ParseDict(default, _literals_pb2.Literal()))

    launch_workflow(token, wf_id, literals)

    return wf_name
