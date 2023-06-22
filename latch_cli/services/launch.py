"""Service to launch a workflow."""

import importlib.util
import typing
from pathlib import Path
from typing import Optional, Tuple, Union

import google.protobuf.json_format as gpjson
import requests
from flyteidl.core.types_pb2 import LiteralType
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from latch_sdk_config.latch import config
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from latch_cli.utils import current_workspace, retrieve_or_login


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

    token = retrieve_or_login()

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

    wf_id, wf_interface, _ = _get_workflow_interface(token, wf_name, version)

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

    _launch_workflow(token, wf_id, wf_literals)
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


def _get_workflow_interface(
    token: str, wf_name: str, version: Union[None, str]
) -> Tuple[int, dict]:
    """Retrieves the set of idl parameter values for a given workflow by name.

    Returns workflow id + interface as JSON string.
    """

    headers = {"Authorization": f"Bearer {token}"}
    _interface_request = {
        "workflow_name": wf_name,
        "version": version,
        "ws_account_id": current_workspace(),
    }

    url = config.api.workflow.interface

    # TODO(ayush) - figure out why timeout within this endpoint only.
    session = requests.Session()
    retries = 5
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        method_whitelist=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    response = session.post(url, headers=headers, json=_interface_request)

    wf_interface_resp = response.json()

    wf_id, wf_interface, wf_default_params = (
        wf_interface_resp.get("id"),
        wf_interface_resp.get("interface"),
        wf_interface_resp.get("default_params"),
    )
    if wf_interface is None:
        raise ValueError(
            "Could not find interface. Nucleus returned a malformed JSON response -"
            f" {wf_interface_resp}"
        )
    if wf_id is None:
        raise ValueError(
            "Could not find wf ID. Nucleus returned a malformed JSON response -"
            f" {wf_interface_resp}"
        )
    if wf_default_params is None:
        raise ValueError(
            "Could not find wf default parameters. Nucleus returned a malformed JSON"
            f" response - {wf_interface_resp}"
        )

    return int(wf_id), wf_interface, wf_default_params


def _launch_workflow(token: str, wf_id: str, params: dict) -> bool:
    """Launch the workflow of given id with parameter map.

    Return True if success
    """

    # TODO (kenny) - pull out to consolidated requests class
    # Server sometimes stalls on requests with python user-agent
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like"
            " Gecko) Chrome/72.0.3626.119 Safari/537.36"
        ),
    }

    _interface_request = {
        "workflow_id": str(wf_id),
        "params": params,
        "ws_account_id": current_workspace(),
    }
    url = config.api.execution.create

    response = requests.post(url, headers=headers, json=_interface_request)

    if response.status_code == 403:
        raise PermissionError(
            "You need access to the latch sdk beta ~ join the waitlist @"
            " https://latch.bio/sdk"
        )
    elif response.status_code == 401:
        raise ValueError(
            "your token has expired - please run latch login to refresh your token and"
            " try again."
        )
    wf_interface_resp = response.json()

    return wf_interface_resp.get("success") is True
