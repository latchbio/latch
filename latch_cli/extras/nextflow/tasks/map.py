import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from flytekit.configuration import SerializationSettings
from flytekit.core.constants import SdkTaskType
from flytekit.core.context_manager import (
    ExecutionState,
    FlyteContext,
    FlyteContextManager,
)
from flytekit.core.interface import transform_interface_to_list_interface
from flytekit.core.python_auto_container import PythonAutoContainerTask
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models.array_job import ArrayJob
from flytekit.models.interface import Variable
from flytekit.models.task import Container, K8sPod, Sql
from flytekitplugins.pod.task import Pod

from .base import NextflowBaseTaskResolver, NFTaskType
from .process import NextflowProcessTask


class MapContainerTask(PythonAutoContainerTask[Pod]):
    def __init__(
        self,
        container_task: PythonAutoContainerTask,
        concurrency: int = 0,
        min_success_ratio: float = 1.0,
    ):
        name = f"mapper_{container_task.name}"

        self._max_concurrency = concurrency
        self._min_success_ratio = min_success_ratio
        self.container_task = container_task
        collection_interface = transform_interface_to_list_interface(
            container_task.python_interface
        )

        self.nf_task_type = NFTaskType.Process

        super().__init__(
            name=name,
            interface=collection_interface,
            task_type=SdkTaskType.CONTAINER_ARRAY_TASK,
            task_type_version=1,
            task_config=None,
            task_resolver=NextflowBaseTaskResolver(),
        )

    def get_command(self, settings: SerializationSettings) -> List[str]:
        return [
            "pyflyte-map-execute",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
            "--checkpoint-path",
            "{{.checkpointOutputPrefix}}",
            "--prev-checkpoint",
            "{{.prevCheckpointPrefix}}",
            "--resolver",
            self.container_task.task_resolver.location,
            "--",
            *self.container_task.task_resolver.loader_args(
                settings, self.container_task
            ),
        ]

    def get_container(self, settings: SerializationSettings) -> Container:
        with self.prepare_target():
            return self.container_task.get_container(settings)

    def get_k8s_pod(self, settings: SerializationSettings) -> Optional[K8sPod]:
        with self.prepare_target():
            return self.container_task.get_k8s_pod(settings)

    def get_sql(self, settings: SerializationSettings) -> Optional[Sql]:
        with self.prepare_target():
            return self.container_task.get_sql(settings)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return ArrayJob(
            parallelism=self._max_concurrency, min_success_ratio=self._min_success_ratio
        ).to_dict()

    def get_config(self, settings: SerializationSettings) -> Optional[Dict[str, str]]:
        return self.container_task.get_config(settings)

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if (
            ctx is not None
            and ctx.execution_state is not None
            and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION
        ):
            return self._execute_map_task(ctx, **kwargs)

        return self._raw_execute(**kwargs)

    @contextmanager
    def prepare_target(self):
        """
        Alters the underlying run_task command to modify it for map task execution and then resets it after.
        """
        self.container_task.set_command_fn(self.get_command)
        try:
            yield
        finally:
            self.container_task.reset_command_fn()

    @staticmethod
    def _compute_array_job_index() -> int:
        return int(os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET", 0)) + int(
            os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME", ""), 0)
        )

    @property
    def _outputs_interface(self) -> Dict[Any, Variable]:
        return self.interface.outputs

    def get_type_for_output_var(self, k: str, v: Any) -> Optional[Type[Any]]:
        return self.interface.outputs[k]

    def get_fn_interface(self):
        assert isinstance(self.container_task, NextflowProcessTask)
        return self.container_task.get_fn_interface()

    def get_fn_return_stmt(self):
        assert isinstance(self.container_task, NextflowProcessTask)
        return self.container_task.get_fn_return_stmt()

    def get_fn_code(self, nf_path_in_container: Path):
        assert isinstance(self.container_task, NextflowProcessTask)
        return self.container_task.get_fn_code(nf_path_in_container)

    def _execute_map_task(self, ctx: FlyteContext, **kwargs) -> Any:
        """
        This is called during ExecutionState.Mode.TASK_EXECUTION executions, that is executions orchestrated by the
        Flyte platform. Individual instances of the map task, aka array task jobs are passed the full set of inputs but
        only produce a single output based on the map task (array task) instance. The array plugin handler will actually
        create a collection from these individual outputs as the final map task output value.
        """
        task_index = self._compute_array_job_index()
        map_task_inputs = {}
        for k in self.interface.inputs.keys():
            map_task_inputs[k] = kwargs[k][task_index]

        return self.container_task.execute(**map_task_inputs)

    def _raw_execute(self, **kwargs) -> Any:
        """
        This is called during locally run executions. Unlike array task execution on the Flyte platform, _raw_execute
        produces the full output collection.
        """
        outputs_expected = True
        if not self.interface.outputs:
            outputs_expected = False
        outputs = []

        any_input_key = (
            list(self.container_task.interface.inputs.keys())[0]
            if self.container_task.interface.inputs.items() is not None
            else None
        )

        for i in range(len(kwargs[any_input_key])):
            single_instance_inputs = {}
            for k in self.interface.inputs.keys():
                single_instance_inputs[k] = kwargs[k][i]
            o = exception_scopes.user_entry_point(self.container_task.execute)(
                **single_instance_inputs
            )
            if outputs_expected:
                outputs.append(o)

        return outputs
