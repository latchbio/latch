"""Service to launch a workflow."""

import importlib.util
import typing
from pathlib import Path
from typing import Optional

import google.protobuf.json_format as gpjson
from flyteidl.core.types_pb2 import LiteralType
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine

from latch.utils import current_workspace

from .interface import get_workflow_interface
from .launch_v2 import launch_workflow


def launch(params_file: Path, version: Optional[str] = None) -> str:
    """Launches a (versioned) workflow with parameters specified in python.

    Using a parameter map written in python (this can be generated for you with
    `get_params`), this function will launch the workflow specified in the file
    using the parameters therein. This function also accepts an optional
    `version` parameter to further specify the workflow to be run. If it is not
    provided, this function will default to running the latest version of the
    specified workflow.

    Args:
        params_file: A path pointing to a python parameter file containing a
            function call that represents the workflow execution with valid
            parameter values.
        version: An optional workflow version to launch, defaulting to the
            latest if not provided.

    Returns:
        The name of the workflow.

    Example:
        >>> launch(Path("wf.__init__.assemble_and_sort.params.py"))
            # Launches an execution of `wf.__init__.assemble_and_sort` with the
            # parameters specified in the referenced file.
    """

    with open(params_file, "r") as pf:
        param_code = pf.read()
        spec = importlib.util.spec_from_loader("wf_params", loader=None)
        param_module = importlib.util.module_from_spec(spec)
        exec(param_code, param_module.__dict__)

    module_vars = vars(param_module)
    try:
        wf_params = module_vars["params"]
    except KeyError as e:
        raise ValueError(
            f"Execution file {params_file.name} needs to have"
            " a parameter value dictionary named 'params'"
        ) from e

    wf_name = wf_params.get("_name")
    if wf_name is None:
        raise ValueError(
            f"The dictionary of parameters in the launch file lacks the"
            f" _name key used to identify the workflow. Make sure a _name"
            f" key with the workflow name exists in the dictionary."
        )

    workspace_id = current_workspace()

    wf_id, wf_interface, _ = get_workflow_interface(workspace_id, wf_name, version)

    wf_vars = wf_interface["variables"]
    wf_literals = {}
    for key, value in wf_vars.items():
        ctx = FlyteContextManager.current_context()
        literal_type_json = value["type"]
        literal_type = gpjson.ParseDict(literal_type_json, LiteralType())

        if key in wf_params:
            python_value = wf_params[key]
            # Recover parameterized generics for TypeTransformer.
            python_type = _guess_python_type(python_value)

            python_type_literal = TypeEngine.to_literal(
                ctx, python_value, python_type, literal_type
            )

            wf_literals[key] = gpjson.MessageToDict(python_type_literal.to_flyte_idl())

    launch_workflow(workspace_id, wf_id, wf_literals)
    return wf_name


def _guess_python_type(v: any) -> typing.T:
    """Python literal guesser.

    We will attempt to construct the correct python type representation from the
    value and JSON type representation and rely on the TypeTransformer to produce
    the correct flyte literal for execution (FlyteIDL representation of the value).
    This is essentially how flytekit does it.

    Using the type() function alone is not sufficient because flyte interprets
    the python list literal as a generic collection type and needs a
    parameterization.

    For example:

    ..
       >> type(["AUG", "AAA"]) = list
       <class 'list'>

    Becomes List[str] s.t.

    ..
       >> TypeEngine.to_literal(ctx, ["AUG", "AAA"], List[str], type_literal)

    Returns our desired flyte literal.
    """

    if type(v) is list:
        if len(v) == 0:
            return typing.List[None]
        elif type(v[0]) is list:
            return typing.List[_guess_python_type(v[0])]
        else:
            return typing.List[type(v[0])]

    # TODO: maps, Records, future complex types

    return type(v)
