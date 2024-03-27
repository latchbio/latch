import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Union

from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.task import TaskPlugins
from flytekit.exceptions import scopes as exception_scopes
from flytekitplugins.pod import Pod
from flytekitplugins.pod.task import PodFunctionTask


def dynamic_resource_task(
    cpu: Union[int, Callable], memory: Union[int, Callable], disk: Union[int, Callable]
):
    def f(**kwargs):
        res = {
            "cpu": cpu(**kwargs) if callable(cpu) else cpu,
            "memory": memory(**kwargs) if callable(memory) else memory,
            "disk": disk(**kwargs) if callable(disk) else disk,
        }
        # todo: perform some validation on the results ex. > 0 (within resonable limits)
        print("UPDATING CRD WITH RESULTS: ", res)

    return f


@dataclass(frozen=True)
class DynamicTaskConfig:
    pre_task_function: Callable
    pod_config: Pod


class DynamicPythonFunctionTask(PodFunctionTask):
    def __init__(
        self, task_config: DynamicTaskConfig, task_function: Callable, **kwargs
    ):
        self._pre_task_function = task_config.pre_task_function
        # todo: validate the function signatures
        super().__init__(
            task_config=task_config.pod_config, task_function=task_function, **kwargs
        )

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        if os.environ.get("FLYTE_PRE_EXECUTE") is not None:
            self._interface._outputs = {}

    def execute(self, **kwargs):
        if os.environ.get("FLYTE_PRE_EXECUTE") is not None:
            return exception_scopes.user_entry_point(self._pre_task_function)(**kwargs)
        return super().execute(**kwargs)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {"preExecEnabled": True}


TaskPlugins.register_pythontask_plugin(DynamicTaskConfig, DynamicPythonFunctionTask)
