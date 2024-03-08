import importlib
from pathlib import Path
from typing import Any, Dict, List, Mapping, Type, cast

from flytekit.configuration import SerializationSettings
from flytekit.core.constants import SdkTaskType
from flytekit.core.interface import Interface
from flytekit.core.python_auto_container import (
    DefaultTaskResolver,
    PythonAutoContainerTask,
)
from flytekit.models import task as _task_models
from flytekitplugins.pod.task import (
    _PRIMARY_CONTAINER_NAME_FIELD,
    Pod,
    _sanitize_resource_name,
)
from kubernetes.client import ApiClient
from kubernetes.client.models import V1Container, V1EnvVar, V1ResourceRequirements

from latch.resources.tasks import custom_task
from latch.types.metadata import ParameterType

from ...common.utils import reindent
from ..workflow import NextflowWorkflow


class NextflowBaseTask(PythonAutoContainerTask[Pod]):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
        # todo(ayush): expose / infer these somehow
        cpu: int = 4,
        memory: int = 8,
    ):
        self.id = id
        self.wf = wf

        interface = Interface(inputs, outputs, docstring=None)
        self._python_inputs = inputs
        self._python_outputs = outputs

        self.wf_inputs: Dict[str, Type] = {}
        self.conditional_inputs: Dict[str, Type] = {}
        self.channel_inputs: Dict[str, Type] = {}

        for k, v in inputs.items():
            if k.startswith("wf_"):
                self.wf_inputs[k] = v
            elif k.startswith("condition_"):
                self.conditional_inputs[k] = v
            else:
                self.channel_inputs[k] = v

        self.branches = branches

        super().__init__(
            task_type=SdkTaskType.SIDECAR_TASK,
            task_type_version=2,
            name=f"{name}_{id}",
            interface=interface,
            task_config=custom_task(cpu=cpu, memory=memory).keywords["task_config"],
            task_resolver=NextflowBaseTaskResolver(),
        )

    def _serialize_pod_spec(self, settings: SerializationSettings) -> Dict[str, Any]:
        containers = cast(List[V1Container], self.task_config.pod_spec.containers)
        primary_exists = False
        for container in containers:
            if container.name == self.task_config.primary_container_name:
                primary_exists = True
                break
        if not primary_exists:
            # insert a placeholder primary container if it is not defined in the pod spec.
            containers.append(V1Container(name=self.task_config.primary_container_name))

        final_containers = []
        def_container = super().get_container(settings)
        for container in containers:
            # In the case of the primary container, we overwrite specific container attributes with the default values
            # used in the regular Python task.
            if container.name == self.task_config.primary_container_name:
                container.image = def_container.image
                # Spawn entrypoint as child process so it can receive signals
                container.command = def_container.args
                container.args = []

                limits, requests = {}, {}
                for resource in def_container.resources.limits:
                    limits[_sanitize_resource_name(resource)] = resource.value
                for resource in def_container.resources.requests:
                    requests[_sanitize_resource_name(resource)] = resource.value

                resource_requirements = V1ResourceRequirements(
                    limits=limits, requests=requests
                )
                if len(limits) > 0 or len(requests) > 0:
                    # Important! Only copy over resource requirements if they are non-empty.
                    container.resources = resource_requirements

                container.env = [
                    V1EnvVar(name=key, value=val)
                    for key, val in def_container.env.items()
                ]

            final_containers.append(container)

        self.task_config._pod_spec.containers = final_containers

        return ApiClient().sanitize_for_serialization(self.task_config.pod_spec)

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        return {_PRIMARY_CONTAINER_NAME_FIELD: self.task_config.primary_container_name}

    def get_container(self, settings: SerializationSettings) -> None:
        return None

    def get_k8s_pod(self, settings: SerializationSettings) -> _task_models.K8sPod:
        return _task_models.K8sPod(
            pod_spec=self._serialize_pod_spec(settings),
            metadata=_task_models.K8sObjectMetadata(
                labels=self.task_config.labels,
                annotations=self.task_config.annotations,
            ),
        )

    def get_fn_conditions(self):
        res: List[str] = []
        for k in self.conditional_inputs.keys():
            res.append(f"({k} == {self.branches[k]})")
        for k in self.channel_inputs.keys():
            res.append(f"({k} is not None)")

        if len(res) == 0:
            return reindent(
                f"""\
                cond = True

                if cond:
                """,
                1,
            )

        return reindent(
            f"""\
            cond = ({' and '.join(res)})

            if cond:
            """,
            1,
        )

    def get_fn_code(self, nf_script_path_in_container: Path) -> str:
        raise NotImplementedError()

    def execute(self): ...


class NextflowBaseTaskResolver(DefaultTaskResolver):
    @property
    def location(self) -> str:
        return "flytekit.core.python_auto_container.default_task_resolver"

    def loader_args(
        self, settings: SerializationSettings, task: NextflowBaseTask
    ) -> List[str]:
        return ["task-module", "nf_entrypoint", "task-name", task.name]

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        _, task_module, _, task_name, *_ = loader_args

        task_module = importlib.import_module(task_module)

        task_def = getattr(task_module, task_name)
        return task_def
