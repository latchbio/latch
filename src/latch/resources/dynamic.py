import os
from dataclasses import dataclass
from textwrap import dedent
from typing import Any, Callable, Dict, Union

import click
import gql
from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.task import TaskPlugins
from flytekit.exceptions import scopes as exception_scopes
from flytekitplugins.pod import Pod
from flytekitplugins.pod.task import PodFunctionTask
from latch_sdk_gql.execute import execute

from ..executions import get_task_identifier


def _override_task_resources(task_config: Pod) -> None:
    task_id = get_task_identifier()
    if task_id is None:
        raise RuntimeError("Could not determine task identifier")

    resources = {
        "tolerations": [
            toleration.to_dict() for toleration in task_config.pod_spec.tolerations
        ],
        "resources": {
            container.name: container.resources.to_dict()
            for container in task_config.pod_spec.containers
        },
    }

    execute(
        gql.gql("""
            mutation OverrideTaskResources(
                $argToken: String!
                $argNodeName: String!,
                $argRetry: BigInt!,
                $argArrIndex: BigInt,
                $argResources: JSON!
            ) {
                overrideTaskResourcesByToken(
                    input: {
                        argToken: $argToken,
                        argNodeName: $argNodeName,
                        argRetry: $argRetry,
                        argArrIndex: $argArrIndex,
                        argResources: $argResources
                    }
                ) {
                    clientMutationId
                }
            }
        """),
        {
            "argToken": task_id.token,
            "argNodeName": task_id.node_name,
            "argRetry": task_id.retry,
            "argArrIndex": task_id.arr_index,
            "argResources": resources,
        },
    )


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

        new_task_config = _custom_task_config(
            int(res["cpu"]), int(res["memory"]), int(res["disk"])
        )
        _override_task_resources(new_task_config)

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

            task_func_ann = task_function.__annotations__
            for name, typ in resource.__annotations__.items():
                if name == "return":
                    continue

                if name not in task_func_ann or task_func_ann[name] != typ:
                    click.secho(
                        dedent(f"""
                        Invalid parameter for resource function `{resource.__name__}`
                        Param `{name}` does not exist or has invalid type {typ}"
                        """),
                        fg="red",
                    )
                    raise click.exceptions.Exit(1)

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
