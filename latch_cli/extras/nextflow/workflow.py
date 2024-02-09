import glob
import importlib
import json
import os
import subprocess
import sys
import textwrap
import typing
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields, is_dataclass, make_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union, get_args, get_origin

import boto3
import click
from flytekit.configuration import SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.constants import SdkTaskType
from flytekit.core.context_manager import (
    ExecutionState,
    FlyteContext,
    FlyteContextManager,
)
from flytekit.core.docstring import Docstring
from flytekit.core.interface import (
    Interface,
    transform_interface_to_list_interface,
    transform_variable_map,
)
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise
from flytekit.core.python_auto_container import (
    DefaultTaskResolver,
    PythonAutoContainerTask,
)
from flytekit.core.workflow import (
    WorkflowBase,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models import literals as literals_models
from flytekit.models import task as _task_models
from flytekit.models.array_job import ArrayJob
from flytekit.models.interface import Variable
from flytekit.models.task import Container, K8sPod, Sql
from flytekitplugins.pod.task import (
    _PRIMARY_CONTAINER_NAME_FIELD,
    Pod,
    _sanitize_resource_name,
)
from kubernetes.client import ApiClient
from kubernetes.client.models import (
    V1Container,
    V1EnvVar,
    V1PodSpec,
    V1ResourceRequirements,
    V1Toleration,
)

from latch.resources.tasks import custom_task
from latch.types import metadata
from latch.types.metadata import NextflowFileParameter, ParameterType, _IsDataclass
from latch_cli import tinyrequests

from ...click_utils import italic
from ...menus import select_tui
from ...utils import identifier_from_str
from ..common.serialize import binding_from_python
from ..common.utils import reindent, type_repr
from .dag import DAG, Vertex, VertexType
from .types import (
    NextflowDAGEdge,
    NextflowDAGVertex,
    NextflowInputParamType,
    NextflowOutputParamType,
    NextflowParam,
)
from .utils import format_param_name


class NextflowWorkflow(WorkflowBase, ClassStorageTaskResolver):
    def __init__(self, dag: DAG):
        # todo(ayush): consolidate w/ snakemake

        assert metadata._nextflow_metadata is not None

        docstring = Docstring(
            f"{metadata._nextflow_metadata.display_name}\n\nSample Description\n\n"
            + str(metadata._nextflow_metadata)
        )
        python_interface = Interface(
            {
                k: (v.type, v.default) if v.default is not None else v.type
                for k, v in metadata._nextflow_metadata.parameters.items()
                if v.type is not None
            },
            {},
            docstring=docstring,
        )

        self.flags_to_params = {
            f"--{k}": v.path if isinstance(v, NextflowFileParameter) else f"wf_{k}"
            for k, v in metadata._nextflow_metadata.parameters.items()
        }

        self.downloadable_params = {
            k: str(v.path)
            for k, v in metadata._nextflow_metadata.parameters.items()
            if isinstance(v, NextflowFileParameter) and v.download
        }

        name = metadata._nextflow_metadata.name
        assert name is not None

        super().__init__(
            name=name,
            workflow_metadata=WorkflowMetadata(
                on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY
            ),
            workflow_metadata_defaults=WorkflowMetadataDefaults(False),
            python_interface=python_interface,
        )

        self.nextflow_tasks: List[NextflowTask] = []

        self.dag = dag

        self.build_from_nextflow_dag()

    def build_from_nextflow_dag(self):
        global_start_node = Node(
            id=_common_constants.GLOBAL_INPUT_NODE_ID,
            metadata=None,
            bindings=[],
            upstream_nodes=[],
            flyte_entity=None,
        )

        interface_inputs = transform_variable_map(self.python_interface.inputs)

        main_task_bindings = []
        for k in self.python_interface.inputs:
            var = interface_inputs[k]
            promise_to_bind = Promise(
                var=k,
                val=NodeOutput(node=global_start_node, var=k),
            )
            main_task_bindings.append(
                binding_from_python(
                    var_name=k,
                    expected_literal_type=var.type,
                    t_value=promise_to_bind,
                    t_value_type=interface_inputs[k],
                )
            )

        # wf input files that need to be downloaded into every task
        global_wf_inputs = {
            f"wf_{k}": v for k, v in self.python_interface.inputs.items()
        }
        global_wf_input_bindings = [
            binding_from_python(
                var_name=f"wf_{k}",
                expected_literal_type=interface_inputs[k].type,
                t_value=Promise(
                    var=k,
                    val=NodeOutput(node=global_start_node, var=k),
                ),
                t_value_type=interface_inputs[k],
            )
            for k, v in self.python_interface.inputs.items()
        ]

        node_map: Dict[str, Node] = {}
        extra_nodes: List[Node] = []

        for vertex in self.dag.toposorted():
            upstream_nodes = [global_start_node]

            task_inputs = {**global_wf_inputs}

            if len(vertex.outputNames) > 0:
                task_outputs = {o: Optional[str] for o in vertex.outputNames}
            else:
                task_outputs = {"res": Optional[str]}

            task_bindings: List[literals_models.Binding] = [*global_wf_input_bindings]
            branches: Dict[str, bool] = {}
            for dep, edge in self.dag.ancestors()[vertex]:
                if dep.type == VertexType.Conditional:
                    param_name = f"condition_{dep.id}"
                    task_inputs[param_name] = Optional[bool]

                    assert (
                        edge.branch is not None
                    ), f"Edge: {edge}, Dep: {dep.id}, Vertex: {vertex.id}"
                    branches[param_name] = edge.branch

                    node = NodeOutput(node=node_map[dep.id], var=f"condition")
                else:
                    param_name = f"c{dep.id}"
                    var = "res"
                    for o in dep.outputNames:
                        if edge.label.endswith(o):
                            param_name = var = o
                            break

                    task_inputs[param_name] = Optional[str]

                    node = NodeOutput(node=node_map[dep.id], var=var)

                task_bindings.append(
                    literals_models.Binding(
                        var=param_name,
                        binding=literals_models.BindingData(
                            promise=Promise(var=param_name, val=node).ref
                        ),
                    )
                )

                upstream_nodes.append(node_map[dep.id])

            if vertex.type == VertexType.Process:
                pre_adapter_task = NextflowProcessPreAdapterTask(
                    inputs=task_inputs,
                    id=f"{vertex.id}_pre",
                    name=f"pre_adapter_{identifier_from_str(vertex.label)}",
                    branches=branches,
                    wf=self,
                )
                self.nextflow_tasks.append(pre_adapter_task)

                pre_adapter_node = Node(
                    id=f"n{vertex.id}-pre-adapter",
                    metadata=pre_adapter_task.construct_node_metadata(),
                    bindings=sorted(task_bindings, key=lambda b: b.var),
                    upstream_nodes=upstream_nodes,
                    flyte_entity=pre_adapter_task,
                )
                extra_nodes.append(pre_adapter_node)

                post_adapter_task = NextflowProcessPostAdapterTask(
                    outputs=task_outputs,
                    id=f"{vertex.id}_post",
                    name=f"post_adapter_{identifier_from_str(vertex.label)}",
                    wf=self,
                )

                self.nextflow_tasks.append(post_adapter_task)

                # todo: maybe validate dataclass fields are contiguous sequences of
                # integers starting at 0

                def parse_dataclass(
                    x: Union[List[Type[_IsDataclass]], None],
                ) -> Tuple[Union[Type[_IsDataclass], None], int]:
                    if get_origin(x) is not list:
                        return None, 0

                    d = get_args(x)[0]

                    if not is_dataclass(d):
                        return d, 0

                    num_fields = 0
                    for f in fields(d):
                        if f.name.startswith("wf_"):
                            continue

                        num_fields += 1

                    return d, num_fields

                input_dataclass, num_inputs = parse_dataclass(
                    pre_adapter_task._python_outputs["default"]
                )
                output_dataclass, num_outputs = parse_dataclass(
                    post_adapter_task._python_inputs["default"]
                )

                process_task = NextflowProcessTask(
                    inputs={"default": input_dataclass},
                    outputs={"o0": output_dataclass},
                    num_inputs=num_inputs,
                    num_outputs=num_outputs,
                    id=vertex.id,
                    name=identifier_from_str(vertex.label),
                    code=vertex.label,
                    statement=vertex.statement,
                    ret=vertex.ret,
                    wf=self,
                )

                mapped_process_task = MapContainerTask(process_task)

                self.nextflow_tasks.append(process_task)

                mapped_process_node = Node(
                    id=f"n{vertex.id}",
                    metadata=mapped_process_task.construct_node_metadata(),
                    bindings=[
                        literals_models.Binding(
                            var="default",
                            binding=literals_models.BindingData(
                                promise=Promise(
                                    var="default",
                                    val=NodeOutput(
                                        node=pre_adapter_node,
                                        var="default",
                                    ),
                                ).ref
                            ),
                        )
                    ],
                    upstream_nodes=[pre_adapter_node],
                    flyte_entity=mapped_process_task,
                )

                # adapter tasks are ordered before process task because of
                # dependent types defined in code generated from adapters

                extra_nodes.append(mapped_process_node)

                post_adapter_node = Node(
                    id=f"n{vertex.id}-post-adapter",
                    metadata=post_adapter_task.construct_node_metadata(),
                    bindings=[
                        literals_models.Binding(
                            var="default",
                            binding=literals_models.BindingData(
                                promise=Promise(
                                    var="default",
                                    val=NodeOutput(
                                        node=mapped_process_node,
                                        var="o0",
                                    ),
                                ).ref
                            ),
                        )
                    ],
                    upstream_nodes=[mapped_process_node],
                    flyte_entity=post_adapter_task,
                )

                node_map[vertex.id] = post_adapter_node

            elif vertex.type in VertexType.Conditional:
                conditional_task = NextflowConditionalTask(
                    task_inputs,
                    vertex.id,
                    f"conditional_{vertex.label}",
                    vertex.statement,
                    vertex.ret,
                    branches,
                    self,
                )
                self.nextflow_tasks.append(conditional_task)

                node = Node(
                    id=f"n{vertex.id}",
                    metadata=conditional_task.construct_node_metadata(),
                    bindings=task_bindings,
                    upstream_nodes=upstream_nodes,
                    flyte_entity=conditional_task,
                )

                node_map[vertex.id] = node

            # elif vertex.type == VertexType.Operator:
            else:
                operator_task = NextflowOperatorTask(
                    inputs=task_inputs,
                    outputs=task_outputs,
                    name=vertex.label,
                    id=vertex.id,
                    statement=vertex.statement,
                    ret=vertex.ret,
                    branches=branches,
                    wf=self,
                )
                self.nextflow_tasks.append(operator_task)

                node = Node(
                    id=f"n{vertex.id}",
                    metadata=operator_task.construct_node_metadata(),
                    bindings=task_bindings,
                    upstream_nodes=upstream_nodes,
                    flyte_entity=operator_task,
                )

                node_map[vertex.id] = node

            #     ...

            # else:
            #     raise ValueError(f"Unsupported vertex type for {repr(vertex)}")

        self._nodes = list(node_map.values()) + extra_nodes

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)


class NextflowTask(PythonAutoContainerTask[Pod]):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        outputs: Dict[str, Type[ParameterType]],
        id: str,
        name: str,
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
    ):
        self.id = id
        name = f"{name}_{id}"
        self.wf = wf

        interface = Interface(inputs, outputs, docstring=None)
        self._python_inputs = inputs
        self._python_outputs = outputs

        self.wf_inputs = {}
        self.conditional_inputs = {}
        self.channel_inputs = {}

        for k, v in inputs.items():
            if k.startswith("wf_"):
                self.wf_inputs[k] = v
            elif k.startswith("condition_"):
                self.conditional_inputs[k] = v
            else:
                self.channel_inputs[k] = v

        self.branches = branches

        # todo(ayush): expose / infer these somehow
        cores = 4
        mem = 8589 * 1000 * 1000 // 1024 // 1024 // 1024

        super().__init__(
            task_type=SdkTaskType.SIDECAR_TASK,
            task_type_version=2,
            name=name,
            interface=interface,
            task_config=custom_task(cpu=cores, memory=mem).keywords["task_config"],
            task_resolver=NextflowTaskResolver(),
        )

    def _serialize_pod_spec(self, settings: SerializationSettings) -> Dict[str, Any]:
        containers = self.task_config.pod_spec.containers
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

    def execute(self): ...


class NextflowTaskResolver(DefaultTaskResolver):
    @property
    def location(self) -> str:
        return "flytekit.core.python_auto_container.default_task_resolver"

    def loader_args(
        self, settings: SerializationSettings, task: NextflowTask
    ) -> List[str]:
        return ["task-module", "nf_entrypoint", "task-name", task.name]

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        _, task_module, _, task_name, *_ = loader_args

        task_module = importlib.import_module(task_module)

        task_def = getattr(task_module, task_name)
        return task_def


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

        super().__init__(
            name=name,
            interface=collection_interface,
            task_type=SdkTaskType.CONTAINER_ARRAY_TASK,
            task_type_version=1,
            task_config=None,
            task_resolver=NextflowTaskResolver(),
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

    def get_k8s_pod(self, settings: SerializationSettings) -> K8sPod:
        with self.prepare_target():
            return self.container_task.get_k8s_pod(settings)

    def get_sql(self, settings: SerializationSettings) -> Sql:
        with self.prepare_target():
            return self.container_task.get_sql(settings)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return ArrayJob(
            parallelism=self._max_concurrency, min_success_ratio=self._min_success_ratio
        ).to_dict()

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        return self.container_task.get_config(settings)

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if (
            ctx.execution_state
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
            os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME"))
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

    def get_fn_code(self, nf_path_in_container: str):
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
        return exception_scopes.user_entry_point(self.container_task.execute)(
            **map_task_inputs
        )

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


class NextflowProcessTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        outputs: Dict[str, Type[ParameterType]],
        num_inputs: int,
        num_outputs: int,
        id: int,
        name: str,
        code: str,
        statement: str,
        ret: List[str],
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, {}, wf)

        self.wf_inputs = {}
        self.conditional_inputs = {}
        self.channel_inputs = {}

        for f in fields(inputs["default"]):
            k = f.name
            v = f.type

            if k.startswith("wf_"):
                self.wf_inputs[k] = v
            elif k.startswith("condition_"):
                self.conditional_inputs[k] = v
            else:
                self.channel_inputs[k] = v

        self.code = code
        self.process_name = name

        self.statement = statement
        self.ret = ret

        assert len(self._python_inputs) == 1 and len(self._python_outputs) == 1

        self.num_inputs = num_inputs
        self.num_outputs = num_outputs

    def get_fn_interface(self):
        inputs = list(self._python_inputs.items())[0]
        output_t = list(self._python_outputs.values())[0]

        return reindent(
            rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @task(cache=True)
                def {self.name}(
                    {inputs[0]}: {getattr(inputs[1], "__name__", None)}
                ) -> {getattr(output_t, "__name__", None)}:
                """,
            0,
        )

    def get_fn_return_stmt(self):
        results: List[str] = []

        res_type = list(self._python_outputs.values())[0]

        if res_type is None:
            return "    return None"

        for field in fields(res_type):
            results.append(
                reindent(
                    rf"""
                    {field.name}=out_channels.get(f"{field.name}", "")
                    """,
                    2,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                    return {res_type.__name__}(
                __return_str__
                    )
            """,
            0,
        ).replace("__return_str__", return_str)

    def get_fn_code(self, nf_path_in_container: str):
        code_block = self.get_fn_interface()

        run_task_entrypoint = [
            "/root/nextflow",
            "run",
            nf_path_in_container,
            "-profile",
            "mamba",
        ]

        for flag, val in self.wf.flags_to_params.items():
            run_task_entrypoint.extend([flag, str(val)])

        for k, v in self.wf.downloadable_params.items():
            code_block += reindent(
                f"""
                {k}_p = Path(default.wf_{k}).resolve()
                {k}_dest_p = Path({repr(v)}).resolve()

                check_exists_and_rename(
                    {k}_p,
                    {k}_dest_p
                )

                """,
                1,
            )

        code_block += reindent(
            rf"""

            channel_vals = [{','.join([f"json.loads(default.{x})" for x in self.channel_inputs])}]

            print("\n\n\nRunning nextflow task: {run_task_entrypoint}\n")
            try:
                subprocess.run(
                    [{','.join([f"str(default.{x})" if x.startswith("wf_") else repr(x) for x in run_task_entrypoint])}],
                    env={{
                        **os.environ,
                        "LATCH_EXPRESSION": {repr(self.statement)},
                        "LATCH_RETURN": {repr(json.dumps(self.ret))},
                        **({{"LATCH_PARAM_VALS": json.dumps(channel_vals)}} if len(channel_vals) > 0 else {{}}),
                    }},
                    check=True,
                )
            except Exception as e:
                print("\n\n\n[!] Failed\n\n\n")
                raise e


            out_channels = {{}}
            files = [Path(f) for f in glob.glob(".latch/task-outputs/*.json")]

            for file in files:
                out_channels[file.stem] = file.read_text()
            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block


def dataclass_from_python_params(
    params: Dict[str, Type[ParameterType]], name: str
) -> Type[_IsDataclass]:
    return make_dataclass(cls_name=f"Dataclass_{name}", fields=list(params.items()))


def dataclass_code_from_python_params(
    params: Dict[str, Type[ParameterType]], name: str
):
    cls = dataclass_from_python_params(params, name)

    output_fields = "\n".join(
        reindent(f"{f.name}: {type_repr(f.type)}", 1) for f in fields(cls)
    )

    return reindent(
        rf"""
        @dataclass
        class {cls.__name__}:
        __output_fields__

        """,
        0,
    ).replace("__output_fields__", output_fields)


class NextflowProcessPreAdapterTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        id: str,
        name: str,
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
    ):
        self.num_params = len(inputs)

        if len(inputs) > 0:
            self.dataclass = dataclass_from_python_params(inputs, id)

            outputs = {"default": List[self.dataclass]}
        else:
            outputs = {"default": List[None]}

        super().__init__(inputs, outputs, id, name, branches, wf)
        self.process_name = name

    def get_fn_interface(self):
        res = ""

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: {type_repr(List[t]) if param.startswith("c") else type_repr(t)}
                """,
                1,
            ).rstrip()
            for param, t in self._python_inputs.items()
        )

        if len(self._python_inputs) > 0:
            res += dataclass_code_from_python_params(self._python_inputs, self.id)

        output_typ = self._python_outputs["default"]

        res += reindent(
            rf"""

            class Res_{self.id}(NamedTuple):
                default: {type_repr(output_typ)}

            """,
            0,
        )

        res += reindent(
            rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @task(cache=True)
                def {self.name}(
                __params__
                ) -> Res_{self.id}:
                """,
            0,
        ).replace("__params__", params_str)

        return res

    def get_fn_return_stmt(self):
        return reindent(f"return Res_{self.id}(default=result)", 1)

    def get_fn_code(self, nf_path_in_container: str):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_conditions()

        fs = fields(self.dataclass)

        channel_fields = [f for f in fs if not f.name.startswith("wf_")]
        if len(channel_fields) == 0:
            code_block += reindent(
                f"""
                result = []
                """,
                2,
            )
        else:
            assignment_str = ", ".join(
                [f"{field.name}=x[{i}]" for i, field in enumerate(fs)]
            )
            variables = ", ".join([
                (
                    f"repeat({field.name})"
                    if field.name.startswith("wf_")
                    else f"map(lambda x: json.dumps([x]), json.loads({field.name}))"
                )
                for field in fs
            ])

            code_block += reindent(
                rf"""
                result = [Dataclass_{self.id}({assignment_str}) for x in zip({variables})]
                """,
                2,
            )

        code_block += reindent(
            rf"""
            else:
                result = []

            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block


class NextflowProcessPostAdapterTask(NextflowTask):
    def __init__(
        self,
        outputs: Dict[str, Type[ParameterType]],
        id: str,
        name: str,
        wf: NextflowWorkflow,
    ):
        self.dataclass_code = dataclass_code_from_python_params(outputs, id)
        exec(self.dataclass_code, globals())

        inputs = {"default": List[globals()[f"Dataclass_{id}"]]}
        super().__init__(inputs, outputs, id, name, {}, wf)
        self.process_name = name

    def get_fn_interface(self):
        res = ""

        output_fields = "\n".join(
            reindent(
                rf"""
                {param}:  {type_repr(List[t])}
                """,
                1,
            ).rstrip()
            for param, t in self._python_outputs.items()
        )

        res += reindent(
            rf"""
            class Res{self.name}(NamedTuple):
            __output_fields__

            """,
            0,
        ).replace("__output_fields__", output_fields)

        res += self.dataclass_code

        outputs_str = f"Res{self.name}:"

        res += reindent(
            rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @task(cache=True)
                def {self.name}(
                    default: List[Dataclass_{self.id}]
                ) -> __outputs__
                """,
            0,
        ).replace("__outputs__", outputs_str)
        return res

    def get_fn_return_stmt(self):
        results: List[str] = []
        for out_name in self._python_outputs.keys():
            results.append(
                reindent(
                    rf"""
                    {out_name}=[x.{out_name} for x in default]
                    """,
                    1,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
            return Res{self.name}(
            __return_str__
            )
            """,
            1,
        ).replace("__return_str__", return_str)

    def get_fn_code(self, nf_path_in_container: str):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_return_stmt()
        return code_block


class NextflowOperatorTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        outputs: Dict[str, Type[ParameterType]],
        id: str,
        name: str,
        statement: str,
        ret: List[str],
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
    ):
        self.operator_id = id
        self.statement = statement
        self.ret = ret
        super().__init__(inputs, outputs, id, name, branches, wf)

    def get_fn_interface(self):
        res = ""

        outputs_str = "None:"
        if len(self._python_outputs.items()) > 0:
            output_fields = "\n".join(
                reindent(
                    rf"""
                    {param}: {type_repr(t)}
                    """,
                    1,
                ).rstrip()
                for param, t in self._python_outputs.items()
            )

            res += reindent(
                rf"""
                class Res{self.name}(NamedTuple):
                __output_fields__

                """,
                0,
            ).replace("__output_fields__", output_fields)
            outputs_str = f"Res{self.name}:"

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: {type_repr(t)}
                """,
                1,
            ).rstrip()
            for param, t in self._python_inputs.items()
        )

        res += (
            reindent(
                rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @task(cache=True)
                def {self.name}(
                __params__
                ) -> __outputs__
                """,
                0,
            )
            .replace("__params__", params_str)
            .replace("__outputs__", outputs_str)
        )
        return res

    def get_fn_return_stmt(self):
        results: List[str] = []
        for out_name, out_type in self._python_outputs.items():
            results.append(
                reindent(
                    rf"""
                    {out_name}=out_channels.get("{out_name}", "")
                    """,
                    2,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                    return Res{self.name}(
                __return_str__
                    )
            """,
            0,
        ).replace("__return_str__", return_str)

    def get_fn_code(self, nf_path_in_container: str):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_conditions()

        run_task_entrypoint = [
            "/root/nextflow",
            "run",
            nf_path_in_container,
            "-profile",
            "mamba",
        ]

        for flag, val in self.wf.flags_to_params.items():
            run_task_entrypoint.extend([flag, str(val)])

        for k, v in self.wf.downloadable_params.items():
            code_block += reindent(
                f"""
                {k}_p = Path(wf_{k}).resolve()
                {k}_dest_p = Path({repr(v)}).resolve()

                check_exists_and_rename(
                    {k}_p,
                    {k}_dest_p
                )

                """,
                2,
            )

        code_block += reindent(
            rf"""

                channel_vals = [{", ".join([f"json.loads({x})" for x in self.channel_inputs])}]

                print(repr(channel_vals))

                subprocess.run(
                    [{','.join([f"str({x})" if x.startswith("wf_") else repr(x) for x in run_task_entrypoint])}],
                    env={{
                        **os.environ,
                        "LATCH_EXPRESSION": {repr(self.statement)},
                        "LATCH_RETURN": {repr(json.dumps(self.ret))},
                        **({{"LATCH_PARAM_VALS": json.dumps(channel_vals)}} if len(channel_vals) > 0 else {{}}),
                    }},
                    check=True,
                )

                out_channels = {{}}
                files = [Path(f) for f in glob.glob(".latch/task-outputs/*.json")]

                for file in files:
                    out_channels[file.stem] = file.read_text()
            else:
                out_channels = {{__skip__}}

            """,
            1,
        ).replace(
            "__skip__",
            ", ".join([f"{repr(o)}: None" for o in self._python_outputs.keys()]),
        )

        code_block += self.get_fn_return_stmt()
        return code_block


class NextflowConditionalTask(NextflowOperatorTask):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        id: str,
        name: str,
        statement: str,
        ret: List[str],
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
    ):
        self.operator_id = id

        super().__init__(
            inputs, {"condition": bool}, id, name, statement, ret, branches, wf
        )

    def get_fn_return_stmt(self):
        results: List[str] = []
        for out_name, out_type in self._python_outputs.items():
            results.append(
                reindent(
                    rf"""
                    {out_name}=json.loads(out_channels.get("{out_name}", "true"))
                    """,
                    2,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                    return Res{self.name}(
                __return_str__
                    )
            """,
            0,
        ).replace("__return_str__", return_str)


def build_nf_wf(pkg_root: Path, nf_script: Path) -> NextflowWorkflow:
    nf_executable = pkg_root / ".latch" / "bin" / "nextflow"
    nf_jars = pkg_root / ".latch" / ".nextflow"

    if not nf_executable.exists():
        click.secho("  Downloading Nextflow executable", dim=True)

        res = tinyrequests.get(
            "https://latch-public.s3.us-west-2.amazonaws.com/nextflow"
        )
        nf_executable.parent.mkdir(parents=True, exist_ok=True)

        nf_executable.write_bytes(res.content)
        nf_executable.chmod(0o700)

    if not nf_jars.exists():
        click.secho("  Downloading Nextflow compiled binaries", dim=True)

        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket("latch-public")

        for obj in bucket.objects.filter(Prefix=".nextflow/"):
            obj_path = pkg_root / ".latch" / obj.key
            obj_path.parent.mkdir(parents=True, exist_ok=True)

            bucket.download_file(obj.key, str(obj_path))

    # clear out old dags from previous registers
    old_dag_files = map(Path, glob.glob(str(pkg_root / ".latch" / "*.dag.json")))
    for f in old_dag_files:
        f.unlink()

    try:
        subprocess.run(
            [
                str(pkg_root / ".latch" / "bin" / "nextflow"),
                "-quiet",
                "run",
                str(nf_script),
                "-latchRegister",
            ],
            env={
                "NXF_HOME": str(pkg_root / ".latch" / ".nextflow"),
                "NXF_DISABLE_CHECK_LATEST": "true",
                "NXF_LOG_FILE": "/dev/null",
            },
            check=True,
        )
    except subprocess.CalledProcessError as e:
        click.secho(
            reindent(
                f"""\
                An error occurred while parsing your NF script ({italic(nf_script)})
                Check your script for typos.

                Contact support@latch.bio for help if the issue persists.
                """,
                0,
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    dags: Dict[str, DAG] = {}

    dag_files = map(Path, glob.glob(".latch/*.dag.json"))
    for dag in dag_files:
        wf_name = dag.name.rsplit(".", 2)[0]

        dags[wf_name] = DAG.from_path(dag)

    resolved = DAG.resolve_subworkflows(dags)

    if len(resolved) == 0:
        click.secho("No Nextflow workflows found in this project. Aborting.", fg="red")

        raise click.exceptions.Exit(1)

    dag = list(resolved.values())[0]

    if len(resolved) > 1:
        dag = select_tui(
            "We found multiple independent workflows in this Nextflow project. Which"
            " would you like to register?",
            [
                {
                    "display_name": (
                        k + " (Anonymous Workflow)" if k == "mainWorkflow" else k
                    ),
                    "value": v,
                }
                for k, v in resolved.items()
            ],
        )

        if dag is None:
            click.echo("No workflow selected. Aborting.")

            raise click.exceptions.Exit(0)

    return NextflowWorkflow(dag)


def nf_path_in_container(nf_script: Path, pkg_root: Path) -> str:
    return str(nf_script.resolve())[len(str(pkg_root.resolve())) + 1 :]


def generate_nf_entrypoint(
    wf: NextflowWorkflow,
    pkg_root: Path,
    nf_path: Path,
):
    entrypoint_code_block = reindent(
        r"""
        import os
        from pathlib import Path
        from dataclasses import dataclass, fields
        import shutil
        import subprocess
        from subprocess import CalledProcessError
        import typing
        from typing import NamedTuple, Dict, List
        import stat
        import sys
        import glob
        import re
        import json
        from dataclasses import is_dataclass, asdict
        from enum import Enum
        from itertools import repeat

        from flytekit.extras.persistence import LatchPersistence
        import traceback

        from latch.resources.tasks import custom_task
        from latch.resources.map_tasks import map_task
        from latch.types.directory import LatchDir, LatchOutputDir
        from latch.types.file import LatchFile

        from latch_cli.extras.nextflow.parameters import upload_files, download_files

        from latch_cli.utils import get_parameter_json_value, urljoins, check_exists_and_rename

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)

        """,
        0,
    )

    # entrypoint_code_block += wf.main_task.get_fn_code(
    #     nf_path_in_container(nf_path, pkg_root)
    # )

    for task in wf.nextflow_tasks:
        entrypoint_code_block += (
            task.get_fn_code(nf_path_in_container(nf_path, pkg_root)) + "\n\n"
        )

    entrypoint = pkg_root / ".latch/nf_entrypoint.py"
    entrypoint.write_text(entrypoint_code_block + "\n")
