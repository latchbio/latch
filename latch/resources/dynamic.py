import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Union

from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.task import TaskPlugins
from flytekit.exceptions import scopes as exception_scopes
from flytekitplugins.pod import Pod
from flytekitplugins.pod.task import PodFunctionTask
from latch_sdk_config.latch import config as latch_config

from latch_cli import tinyrequests
from latch_cli.utils import get_auth_header

NUCLEUS_URL = f'https://nucleus.{os.environ.get("LATCH_SDK_DOMAIN", "latch.bio")}'


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

        new_task_config = _custom_task_config(res["cpu"], res["memory"], res["disk"])

        workspace_id = os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")
        if workspace_id is None:
            raise RuntimeError(
                "FLYTE_INTERNAL_EXECUTION_PROJECT environment variable not set"
            )

        try:
            with open("/etc/hostname", "r") as f:
                task_identifier = f.read().strip()
        except FileNotFoundError:
            raise RuntimeError("could not read task identifier from /etc/hostname")

        task_name = os.environ.get("FLYTE_INTERNAL_TASK_NAME")
        if task_name is None:
            raise RuntimeError("FLYTE_INTERNAL_TASK_NAME environment variable not set")

        resp = tinyrequests.post(
            f"{NUCLEUS_URL}/workflows/update-task-resources",
            headers={"Authorization": get_auth_header()},
            json={
                "workspace_id": workspace_id,
                "task_identifier": task_identifier,
                "task_name": task_name,
                "resources": {
                    container.name: container.resources.to_dict()
                    for container in new_task_config.pod_spec.containers
                },
                "tolerations": [
                    toleration.to_dict()
                    for toleration in new_task_config.pod_spec.tolerations
                ],
            },
        )
        resp.raise_for_status()

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
        return {"preExecEnabled": True, "useDynamicResources": True}


TaskPlugins.register_pythontask_plugin(DynamicTaskConfig, DynamicPythonFunctionTask)
