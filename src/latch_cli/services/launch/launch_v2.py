import base64
import json
import typing
from json.decoder import JSONDecodeError
from typing import Any, Optional, Union, get_args, get_origin

import dill
import flyteidl.core.literals_pb2 as pb
import google.protobuf.json_format as gpjson
import gql
from flyteidl.core import interface_pb2 as _interface_pb2
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.promise import translate_inputs_to_literals
from flytekit.core.type_engine import TypeEngine, TypeTransformerFailedError
from flytekit.models.interface import Parameter, ParameterMap, Variable, VariableMap
from flytekit.models.literals import LiteralMap

from latch.utils import current_workspace
from latch_cli import tinyrequests
from latch_cli.services.launch.interface import get_workflow_interface
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import config
from latch_sdk_gql.execute import execute

ExecutionStatus = typing.Literal[
    "UNDEFINED",
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "ABORTED",
    "FAILED",
    "INITIALIZING",
    "WAITING_FOR_RESOURCES",
    "SKIPPED",
    "ABORTING",
]


class Execution:
    def __init__(self, execution_id: str, python_outputs: dict[str, type]) -> None:
        self.execution_id = execution_id
        self.python_outputs = python_outputs

        self.status: ExecutionStatus = "UNDEFINED"

        self.output: Union[dict[str, Any], None] = None
        self.outputs_url: Union[str, None] = None

    @property
    def id(self) -> str:
        return self.execution_id

    def poll_status(self) -> None:
        res: dict[str, Any] = execute(
            gql.gql(
                """
                query GetExecutionStatus($executionId: BigInt!) {
                    executionInfo(id: $executionId) {
                        id
                        status
                        outputsUrl
                    }
                }
                """
            ),
            {"executionId": self.execution_id},
        )
        execution_info = res.get("executionInfo", {})
        self.status = execution_info.get("status")
        self.outputs_url = execution_info.get("outputs_url")

    def get_outputs(self) -> dict[str, Any]:
        if self.output is not None:
            return self.output

        if self.status != "SUCCEEDED" or self.outputs_url is None:
            raise ValueError("workflow non successful")

        res = tinyrequests.post(
            config.api.data.get_flyte_metadata_signed_url,
            headers={"Authorization": get_auth_header()},
            json={"s3_uri": {self.outputs_url}, "action": "get_object"},
        )

        try:
            data = res.json()
        except JSONDecodeError as e:
            raise RuntimeError(
                f"Could not parse response as JSON: ({res.status_code}) {res}"
            ) from e

        if res.status_code != 200:
            print("Error")
            print(data)
            return {}

        presigned_url = data.get("data", {}).get("url")
        output_res = tinyrequests.get(presigned_url)
        output_data = output_res.content

        if output_res.status_code != 200:
            print("Error")
            print(output_data)
            return {}

        output_pb = pb.LiteralMap()
        output_pb.ParseFromString(output_data)
        output_idl = LiteralMap.from_flyte_idl(output_pb)

        output_literals = output_idl.literals
        output: dict[str, Any] = {}

        ctx = FlyteContextManager.current_context()
        assert ctx is not None

        for k, t in self.python_outputs.items():
            if k not in output_literals:
                if get_origin(t) is Union and type(None) in get_args(t):
                    output[k] = None
                else:
                    raise ValueError(f"Required parameter '{k}' not provided in params")

            else:
                output[k] = TypeEngine.to_python_value(ctx, output_literals[k], t)

        return output


def launch_from_launch_plan(
    *, wf_name: str, lp_name: str, version: Optional[str] = None
) -> Execution:
    target_account_id = current_workspace()

    wf_id, interface, default_params = get_workflow_interface(
        target_account_id, wf_name, version
    )
    default_params_map: dict[str, Parameter] = ParameterMap.from_flyte_idl(
        gpjson.ParseDict(default_params, _interface_pb2.ParameterMap())
    ).parameters
    parameter_interface: dict[str, Variable] = VariableMap.from_flyte_idl(
        gpjson.ParseDict(interface, _interface_pb2.VariableMap())
    ).variables

    lp_default_resp: dict[str, Any] = execute(
        gql.gql(
            """
            query LaunchPlanDefaultInputs($workflowId: BigInt!, $namePattern: String!) {
                lpInfos(
                    filter: {
                        workflowId: { equalTo: $workflowId },
                        name: { like: $namePattern }
                    }
                ) {
                    nodes {
                        defaultInputs
                    }
                }
            }
            """
        ),
        {"workflowId": wf_id, "namePattern": f"%.{lp_name}"},
    )

    lp_nodes = lp_default_resp.get("lpInfos", {}).get("nodes", [])
    if len(lp_nodes) == 0:
        raise ValueError(
            f"launchplan `{lp_name}` not found for workflow `{wf_name}` (id `{wf_id}`)"
        )

    if len(lp_nodes) > 1:
        raise ValueError(
            f"multiple launchplans with name `{lp_name}` found for workflow `{wf_name}` (id `{wf_id}`)"
        )

    lp_params_map: dict[str, Parameter] = ParameterMap.from_flyte_idl(
        gpjson.ParseDict(
            json.loads(lp_nodes[0].get("defaultInputs")), _interface_pb2.ParameterMap()
        )
    ).parameters

    combined_params_map: dict[str, Any] = {}

    python_outputs: Union[dict[str, type], None] = None

    for k, v in parameter_interface.items():
        if k in lp_params_map:
            lp_param = lp_params_map[k].default
            if lp_param is not None:
                combined_params_map[k] = gpjson.MessageToDict(lp_param.to_flyte_idl())
                continue

        default_param = default_params_map.get(k)
        if default_param is not None:
            default_param_literal = default_param.default
            if default_param_literal is not None:
                combined_params_map[k] = gpjson.MessageToDict(
                    default_param_literal.to_flyte_idl()
                )
                continue

        combined_params_map[k] = {
            "scalar": {
                "union": {
                    "value": {"scalar": {"noneType": {}}},
                    "type": {"simple": "NONE", "structure": {"tag": "none"}},
                }
            }
        }

        description: dict[str, Any] = json.loads(v.description)
        if description.get("idx") != 0:
            continue

        meta = description.get("__workflow_meta__", {}).get("meta", {})

        raw_python_outputs = meta.get("python_outputs")
        if raw_python_outputs is not None:
            try:
                python_outputs = dill.loads(base64.b64decode(raw_python_outputs))
            except dill.UnpicklingError as e:
                raise ValueError(
                    "Failed to decode the workflow python output -- ensure your python version matches the version in the workflow environment"
                ) from e
            break

    if python_outputs is None:
        raise ValueError(
            "No python outputs found -- re-register workflow with latch version >= 2.65.0 in workflow environment"
        )

    return launch_workflow(
        target_account_id, wf_id, combined_params_map, python_outputs
    )


def launch(
    *, wf_name: str, params: dict[str, Any], version: Optional[str] = None
) -> Execution:
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

    flyte_interface_types: dict[str, Variable] = VariableMap.from_flyte_idl(
        gpjson.ParseDict(interface, _interface_pb2.VariableMap())
    ).variables

    python_interface_with_defaults: Union[dict[str, tuple[type, Any]], None] = None
    python_outputs: Union[dict[str, type], None] = None

    for v in flyte_interface_types.values():
        description: dict[str, Any] = json.loads(v.description)
        if description.get("idx") != 0:
            continue

        meta = description.get("__workflow_meta__", {}).get("meta", {})

        raw_python_interface_with_defaults = meta.get("python_interface")
        if raw_python_interface_with_defaults is not None:
            try:
                python_interface_with_defaults = dill.loads(
                    base64.b64decode(raw_python_interface_with_defaults)
                )
            except dill.UnpicklingError as e:
                raise ValueError(
                    "Failed to decode the workflow python interface -- ensure your python version matches the version in the workflow environment"
                ) from e
            break

        raw_python_outputs = meta.get("python_outputs")
        if raw_python_outputs is not None:
            try:
                python_outputs = dill.loads(base64.b64decode(raw_python_outputs))
            except dill.UnpicklingError as e:
                raise ValueError(
                    "Failed to decode the workflow python output -- ensure your python version matches the version in the workflow environment"
                ) from e
            break

    if python_interface_with_defaults is None:
        raise ValueError(
            "No python interface found -- re-register workflow with latch version >= 2.62.0 in workflow environment"
        )

    if python_outputs is None:
        raise ValueError(
            "No python outputs found -- re-register workflow with latch version >= 2.65.0 in workflow environment"
        )

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
            native_types={k: v[0] for k, v in python_interface_with_defaults.items()},
        )
    except TypeTransformerFailedError as e:
        if "is not an instance of" in str(e):
            raise ValueError(
                "Failed to translate inputs to literals -- ensure you are importing the same classes used in the workflow function header"
            ) from e
        raise

    return launch_workflow(
        target_account_id,
        wf_id,
        {k: gpjson.MessageToDict(v.to_flyte_idl()) for k, v in fixed_literals.items()},
        python_outputs,
    )


def launch_workflow(
    target_account_id: str,
    wf_id: str,
    params: dict[str, Any],
    python_outputs: dict[str, type],
) -> Execution:
    """Launch the workflow of given id with parameter map.

    Return exeuction id if success, raises appropriate exceptions on failure.
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
        raise RuntimeError(
            f"Could not parse response as JSON: ({response.status_code}) {response}"
        ) from e

    def extract_error_message(data: dict[str, Any]) -> str:
        if "error" in data:
            error = data["error"]
            source = error.get("source", "unknown")

            error_data = error.get("data", {})
            message = (
                error_data.get("stderr") or error_data.get("message") or str(error_data)
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
    if (
        "error" in response_data
        or response_data.get("status") != "Successfully launched workflow"
    ):
        error_msg = extract_error_message(response_data)
        print(f"\nFormatted error message: {error_msg}")
        raise RuntimeError(f"Workflow launch failed - {error_msg}")

    execution_id = response_data.get("metadata", {}).get("execution_id")
    if execution_id is None:
        raise RuntimeError("Workflow launch failed - no execution id returned")

    return Execution(execution_id=execution_id, python_outputs=python_outputs)
