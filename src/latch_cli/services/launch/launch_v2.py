import asyncio
import base64
import json
from collections.abc import Generator
from dataclasses import dataclass
from json.decoder import JSONDecodeError
from typing import Any, Literal, Optional, Union, get_args, get_origin
from urllib.parse import urljoin

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

from latch.ldata.path import LPath
from latch.utils import current_workspace
from latch_cli import tinyrequests
from latch_cli.services.launch.interface import get_workflow_interface
from latch_cli.services.launch.type_converter import convert_inputs_to_literals
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import NUCLEUS_URL, config
from latch_sdk_gql.execute import execute

ExecutionStatus = Literal[
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


class MissingParameterError(ValueError): ...


def process_output(outputs_url: str, python_outputs: dict[str, type]) -> dict[str, Any]:
    if python_outputs == {}:
        return {}

    res = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/ldata/get-flyte-metadata-signed-url"),
        headers={"Authorization": get_auth_header()},
        json={"s3_uri": outputs_url, "action": "get_object"},
    )

    res.raise_for_status()
    data = res.json()

    presigned_url = data.get("data", {}).get("url")
    assert presigned_url is not None

    output_res = tinyrequests.get(presigned_url)
    output_res.raise_for_status()
    output_data = output_res.content

    output_pb = pb.LiteralMap()
    output_pb.ParseFromString(output_data)
    output_idl = LiteralMap.from_flyte_idl(output_pb)

    output_literals = output_idl.literals
    output: dict[str, Any] = {}

    ctx = FlyteContextManager.current_context()
    assert ctx is not None

    for k, t in python_outputs.items():
        if k in output_literals:
            output[k] = TypeEngine.to_python_value(ctx, output_literals[k], t)

        elif get_origin(t) is Union and type(None) in get_args(t):
            output[k] = None

        else:
            raise MissingParameterError(
                f"Required parameter '{k}' not provided in params"
            )

    return output


def get_ingress_data(
    flytedb_id: Optional[str], execution_id: Optional[str]
) -> list[LPath]:
    query_res: dict[str, Any] = execute(
        gql.gql(
            """
            query ExecutionIngressTag($flytedbId: BigInt, $executionId: BigInt) {
                ldataNodeEvents(
                    filter: {
                        type: { equalTo: INGRESS }
                        or: [
                            {
                                causeExecutionId: { equalTo: $executionId }
                            },
                            {
                                causeExecutionFlytedbId: { equalTo: $flytedbId }
                            }
                        ]
                    }
                ) {
                    nodes {
                        id
                        ldataNode {
                            id
                        }
                    }
                }
            }
            """
        ),
        {"flytedbId": flytedb_id, "executionId": execution_id},
    )

    nodes = query_res.get("ldataNodeEvents", {}).get("nodes", [])
    res: list[LPath] = []
    for n in nodes:
        ldata_node_id: Union[str, None] = n.get("ldataNode", {}).get("id")
        if ldata_node_id is None:
            continue

        res.append(LPath(f"latch://{ldata_node_id}.node"))

    return res


@dataclass(frozen=True)
class CompletedExecution:
    id: str
    output: dict[str, Any]
    ingress_data: list[LPath]
    status: ExecutionStatus


@dataclass
class Execution:
    id: str
    python_outputs: dict[str, type]
    status: ExecutionStatus = "UNDEFINED"
    outputs_url: Union[str, None] = None
    flytedb_id: Union[str, None] = None

    def poll(self) -> Generator[None, Any, None]:
        while True:
            res: dict[str, Any] = execute(
                gql.gql(
                    """
                    query GetExecutionStatus($executionId: BigInt!) {
                        executionInfo(id: $executionId) {
                            id
                            flytedbId
                            status
                            outputsUrl
                        }
                    }
                    """
                ),
                {"executionId": self.id},
            )

            execution_info = res.get("executionInfo", {})
            self.status = execution_info.get("status", "UNDEFINED")
            self.outputs_url = execution_info.get("outputsUrl")
            self.flytedb_id = execution_info.get("flytedbId")

            yield

    async def wait(self) -> Union[CompletedExecution, None]:
        for _ in self.poll():
            if self.status == "SUCCEEDED" and self.outputs_url is not None:
                return CompletedExecution(
                    id=self.id,
                    output=process_output(self.outputs_url, self.python_outputs),
                    ingress_data=get_ingress_data(
                        flytedb_id=self.flytedb_id, execution_id=self.id
                    ),
                    status=self.status,
                )

            if self.status in {"FAILED", "ABORTED"}:
                return CompletedExecution(
                    id=self.id,
                    output={},
                    ingress_data=get_ingress_data(
                        flytedb_id=self.flytedb_id, execution_id=self.id
                    ),
                    status=self.status,
                )

            await asyncio.sleep(1)

        return None


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
        if raw_python_outputs is None:
            print("No python outputs found. If your workflow has outputs, re-register workflow with latch version >= 2.65.1 in workflow environment to access outputs in Execution object.")
            break

        try:
            python_outputs = dill.loads(base64.b64decode(raw_python_outputs))
        except dill.UnpicklingError as e:
            raise RuntimeError(
                "Failed to decode the workflow python output -- ensure your python version matches the version in the workflow environment"
            ) from e
        break

    if python_outputs is None:
        python_outputs = {}

    return launch_workflow(
        target_account_id, wf_id, combined_params_map, python_outputs
    )


def launch(
    *, wf_name: str, params: dict[str, Any], version: Optional[str] = None, best_effort: bool = True
) -> Execution:
    """Create an execution of workflow `wf_name` with version `version` and parameters `params`.

    If version is not provided, the latest version of the workflow will be launched.

    This command is not backwards compatible with workflows registered with latch version < 2.62.0 in the container.

    Args:
        wf_name: Name of workflow to launch (see `.latch/workflow_name` in the workflow directory).
        params: A dictionary of parameters to pass to the workflow.
        version: An optional workflow version to launch, defaulting to latest.
        best_effort: Use best effort to translate inputs to literals if types do not match

    Returns:
        Execution ID of the launched workflow.
    """
    target_account_id = current_workspace()
    wf_id, interface, defaults = get_workflow_interface(target_account_id, wf_name, version)

    flyte_interface_types: dict[str, Variable] = VariableMap.from_flyte_idl(
        gpjson.ParseDict(interface, _interface_pb2.VariableMap())
    ).variables

    python_outputs: Union[dict[str, type], None] = None
    raw_python_interface_with_defaults: Union[str, None] = None

    for v in flyte_interface_types.values():
        description: dict[str, Any] = json.loads(v.description)
        if description.get("idx") != 0:
            continue

        meta = description.get("__workflow_meta__", {}).get("meta", {})
        raw_python_interface_with_defaults = meta.get("python_interface")
        raw_python_outputs = meta.get("python_outputs")

        if raw_python_outputs is None:
            print("No python outputs found. If your workflow has outputs, re-register workflow with latch version >= 2.65.1 in workflow environment to access outputs in Execution object.")
            break

        try:
            python_outputs = dill.loads(base64.b64decode(raw_python_outputs))  # noqa: S301
        except dill.UnpicklingError as e:
            raise RuntimeError("Failed to decode the workflow outputs. Ensure your python version matches the version in the workflow environment.") from e

        break

    if python_outputs is None:
        python_outputs = {}

    params_for_launch: dict[str, Any] = params
    if best_effort:
        fixed_literals = convert_inputs_to_literals(
            params=params,
            flyte_interface_types=flyte_interface_types,
        )
        defaults = defaults["parameters"]

        params_json: dict[str, Any] = {
            k: gpjson.MessageToDict(v.to_flyte_idl()) for k, v in fixed_literals.items()
        }

        def _is_optional_none(var_type: dict[str, Any]) -> bool:
            ut = var_type.get("unionType")
            if ut is None:
                return False

            return any(v.get("simple") == "NONE" for v in ut.get("variants", []))

        for name, entry in defaults.items():
            if name in params_json:
                continue

            default_lit = entry.get("default")
            if default_lit is not None:
                params_json[name] = default_lit
                continue

            var = entry.get("var", {})
            vtype = var.get("type", {})
            if _is_optional_none(vtype):
                params_json[name] = {
                    "scalar": {
                        "union": {
                            "value": {"scalar": {"noneType": {}}},
                            "type": {"simple": "NONE", "structure": {"tag": "none"}},
                        }
                    }
                }
                continue

            raise ValueError(f"Required parameter '{name}' not provided in params")

        params_for_launch = params_json
    else:
        if raw_python_interface_with_defaults is None:
            raise RuntimeError("Missing python interface in workflow metadata. Try using with best_effort=True.")

        python_interface_with_defaults: Union[dict[str, tuple[type, Any]], None] = None
        try:
            python_interface_with_defaults = dill.loads(  # noqa: S301
                base64.b64decode(raw_python_interface_with_defaults)
            )
        except dill.UnpicklingError as e:
            raise RuntimeError(
                "Failed to decode the workflow python interface. Ensure your python version matches the version in the workflow environment."
            ) from e

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
                    "Failed to translate inputs to literals. Ensure you are importing the same classes used in the workflow function header"
                ) from e
            raise

        params_for_launch = {k: gpjson.MessageToDict(v.to_flyte_idl()) for k, v in fixed_literals.items()}

    return launch_workflow(
        target_account_id,
        wf_id,
        params_for_launch,
        python_outputs,
    )


def launch_workflow(
    target_account_id: str,
    wf_id: str,
    params: dict[str, Any],
    python_outputs: dict[str, type],
) -> Execution:
    """Launch the workflow of given id with parameter map.

    Returns: execution ID
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

    return Execution(id=execution_id, python_outputs=python_outputs)
