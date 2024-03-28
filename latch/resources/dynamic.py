import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Union

import boto3
from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.task import TaskPlugins
from flytekit.exceptions import scopes as exception_scopes
from flytekitplugins.pod import Pod
from flytekitplugins.pod.task import PodFunctionTask

from ..types.json import JsonValue

# todo(rahul): get env instead of hardcoding
flyte_bucket_name = "prion-flyte-dev"


def _update_pod_resources(crd: JsonValue, new: Pod) -> JsonValue:
    if "tasks" not in crd:
        raise RuntimeError("no tasks found in CRD")

    task_name = os.environ.get("FLYTE_INTERNAL_TASK_NAME")
    if task_name is None:
        raise RuntimeError("FLYTE_INTERNAL_TASK_NAME not set")

    key = None
    pod = None
    for key, task_template in crd["tasks"].items():
        if "id" in task_template and task_template["id"].get("name") == task_name:
            if "k8sPod" not in task_template:
                raise RuntimeError(f"task does not have k8sPod spec")
            key = key
            pod = task_template["k8sPod"]
            break

    if key is None:
        raise RuntimeError(f"task not found in CRD")

    # override resource limits/requests and tolerations
    pod["podSpec"]["containers"][0]["resources"] = new.pod_spec.containers[
        0
    ].resources.to_dict()

    pod["podSpec"]["tolerations"] = [
        toleration.to_dict() for toleration in new.pod_spec.tolerations
    ]
    pod["podSpec"]["runtimeClassName"] = new.pod_spec.runtime_class_name

    if new.annotations is not None:
        if "metadata" not in pod:
            pod["metadata"] = {}
        pod["metadata"]["annotations"] = new.annotations

    crd["tasks"][key]["k8sPod"] = pod
    return crd


def _dynamic_resource_task(
    cpu: Union[int, Callable], memory: Union[int, Callable], disk: Union[int, Callable]
):
    def f(**kwargs):
        res: Dict[str, int] = {
            "cpu": cpu(**kwargs) if callable(cpu) else cpu,
            "memory": memory(**kwargs) if callable(memory) else memory,
            "disk": disk(**kwargs) if callable(disk) else disk,
        }
        print(f"Updating crd with new resources={res}")

        from .tasks import _custom_task_config

        new_spec = _custom_task_config(res["cpu"], res["memory"], res["disk"])

        workspace_id = os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")
        token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
        if workspace_id is None or token is None:
            raise RuntimeError(
                "FLYTE_INTERNAL_EXECUTION_PROJECT or FLYTE_INTERNAL_EXECUTION_ID"
                " not set"
            )
        file_key = f"metadata/{workspace_id}/development/{token}/crd/crdparts.json"

        s3 = boto3.client("s3")

        # todo(rahul): probably want to add retries here
        response = s3.get_object(Bucket=flyte_bucket_name, Key=file_key)
        json_data = response["Body"].read().decode("utf-8")
        crd = json.loads(json_data)

        updated = _update_pod_resources(crd, new_spec)

        # todo(rahul): probably want to add retries here
        s3.put_object(Bucket=flyte_bucket_name, Key=file_key, Body=json.dumps(updated))

        print("CRD updated successfully")

    return f


@dataclass(frozen=True)
class DynamicTaskConfig:
    cpu: Union[Callable, int]
    memory: Union[Callable, int]
    storage: Union[Callable, int]
    pod_config: Pod


class DynamicPythonFunctionTask(PodFunctionTask):
    def __init__(
        self, task_config: DynamicTaskConfig, task_function: Callable, **kwargs
    ):
        # validate that the task function inputs are the same as the resource functions
        for resource in [task_config.cpu, task_config.memory, task_config.storage]:
            if not callable(resource):
                continue

            resource_func_ann = resource.__annotations__
            for name, typ in task_function.__annotations__.items():
                if name == "return":
                    continue

                if name not in resource_func_ann or resource_func_ann[name] != typ:
                    raise ValueError(
                        f"Resource function {resource.__name__} does not have the same"
                        f" signature as task function {task_function.__name__}. Param"
                        f" name or type do not match for parameter {name}"
                    )

        self._pre_task_function = _dynamic_resource_task(
            task_config.cpu, task_config.memory, task_config.storage
        )

        super().__init__(
            task_config=task_config.pod_config, task_function=task_function, **kwargs
        )

    def pre_execute(self, _: ExecutionParameters) -> ExecutionParameters:
        if os.environ.get("FLYTE_PRE_EXECUTE") is not None:
            self._interface._outputs = {}

    def execute(self, **kwargs):
        if os.environ.get("FLYTE_PRE_EXECUTE") is not None:
            return exception_scopes.user_entry_point(self._pre_task_function)(**kwargs)
        return super().execute(**kwargs)

    def get_custom(self, _: SerializationSettings) -> Dict[str, Any]:
        return {"preExecEnabled": True}


TaskPlugins.register_pythontask_plugin(DynamicTaskConfig, DynamicPythonFunctionTask)
