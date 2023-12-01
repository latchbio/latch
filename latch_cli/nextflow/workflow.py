import importlib
import json
import os
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar
from urllib.parse import urlparse

import click
from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.constants import SdkTaskType
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.docstring import Docstring
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.map_task import MapPythonTask
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
from flytekit.models.interface import Variable
from flytekit.models.literals import Literal
from flytekit.tools.serialize_helpers import persist_registrable_entities
from flytekitplugins.pod.task import Pod

from latch.resources.tasks import custom_task
from latch.types import metadata
from latch.types.metadata import ParameterType
from latch_cli.services.register.register import (
    _print_reg_resp,
    _recursive_list,
    register_serialized_pkg,
)
from latch_cli.snakemake.serialize import should_register_with_admin
from latch_cli.snakemake.serialize_utils import (
    EntityCache,
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from latch_cli.snakemake.workflow import binding_from_python, interface_to_parameters

T = TypeVar("T")


def reindent(x: str, level: int) -> str:
    if x[0] == "\n":
        x = x[1:]
    return textwrap.indent(textwrap.dedent(x), "    " * level)


class VertexType(Enum):
    process = "process"
    operator = "operator"
    origin = "origin"


class NextflowInputParamType(Enum):
    default = "default"
    val = "val"
    path = "path"


class NextflowOutputParamType(Enum):
    stdoutparam = "stdoutparam"
    valueoutparam = "valueoutparam"
    tupleoutparam = "tupleoutparam"
    fileoutparam = "fileoutparam"


NextflowParamType = NextflowInputParamType | NextflowOutputParamType


@dataclass
class NextflowParam:
    name: str
    paramType: NextflowInputParamType | NextflowOutputParamType


@dataclass
class NextflowDAGVertex:
    id: int
    label: None | str
    vertex_type: VertexType
    input_params: None | List[NextflowParam]
    output_params: None | List[NextflowParam]
    code: None | str


@dataclass
class NextflowDAGEdge:
    id: int
    idx: None | int
    label: None | str
    connection: Tuple[int, int]


class NextflowWorkflow(WorkflowBase, ClassStorageTaskResolver):
    out_parameter_name = "o0"  # must be "o0"

    def __init__(
        self,
    ):
        name = "placeholder_nextflow_name"

        # Not set by parent's constructor
        self._name = "placeholder_nextflow_name"

        self.nextflow_tasks = []

        python_interface, literal_map, nodes, output_bindings = (
            self.build_from_nextflow_dag(name)
        )
        self.literal_map = literal_map

        workflow_metadata = WorkflowMetadata(
            on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY
        )
        workflow_metadata_defaults = WorkflowMetadataDefaults(False)

        super().__init__(
            name=name,
            workflow_metadata=workflow_metadata,
            workflow_metadata_defaults=workflow_metadata_defaults,
            python_interface=python_interface,
        )
        # parent's constructor wipes nodes for whatever reason
        self._nodes = nodes
        self._output_bindings = output_bindings

    def build_from_nextflow_dag(self, name: str):
        GLOBAL_START_NODE = Node(
            id=_common_constants.GLOBAL_INPUT_NODE_ID,
            metadata=None,
            bindings=[],
            upstream_nodes=[],
            flyte_entity=None,
        )

        global_inputs: Dict[str, ParameterType] = {}
        global_outputs: Dict[str, ParameterType] = {}

        node_map: Dict[str, Node] = {}
        extra_nodes: List[Node] = []
        for vertex_id in sorted(dependent_vertices.keys()):
            vertex = vertices[vertex_id]
            if vertex.vertex_type == VertexType.origin:
                continue

            upstream_nodes = []
            bindings: List[literals_models.Binding] = []
            python_inputs: Dict[str, ParameterType] = {}

            for edge in edges_idx_by_end[vertex.id]:
                depen_vertex = vertices.get(edge.connection[0])

                if vertex.vertex_type == VertexType.process:
                    if edge.idx is None:
                        raise ValueError(
                            f"Channel {edge} connecting {depen_vertex} and"
                            f" {vertex} with no index value is not allowed."
                        )
                    param_name = vertex.input_params[edge.idx].name
                else:
                    if depen_vertex:
                        param_name = f"node{depen_vertex.id}"
                    else:
                        param_name = "handle_later"

                python_inputs[param_name] = List[str]

                if (
                    depen_vertex is None
                    or depen_vertex.vertex_type == VertexType.origin
                ):
                    node_output = NodeOutput(node=GLOBAL_START_NODE, var=param_name)
                    global_inputs[param_name] = List[str]
                else:
                    if edge.label is None:
                        if depen_vertex.vertex_type == VertexType.process:
                            raise ValueError(
                                f"Channel {edge} connecting {depen_vertex} and"
                                f" {vertex} with no label is not allowed."
                            )
                        source_param = f"n{depen_vertex.id}"
                    else:
                        source_param = edge.label
                        if source_param == "-":
                            source_param = "stdout"

                    print(vertex.id, source_param, param_name)
                    node_output = NodeOutput(
                        node=node_map[depen_vertex.id],
                        var=source_param,
                    )

                promise_to_bind = Promise(
                    var=param_name,
                    val=node_output,
                )
                bindings.append(
                    literals_models.Binding(
                        var=param_name,
                        binding=literals_models.BindingData(
                            promise=promise_to_bind.ref
                        ),
                    )
                )

                if depen_vertex and depen_vertex.id in node_map:
                    upstream_nodes.append(node_map[depen_vertex.id])

            if vertex.vertex_type == VertexType.process:
                pre_adapter_task = NextflowProcessPreAdapterTask(
                    inputs=python_inputs,
                    outputs={"default": List[str]},
                    id=f"{vertex.id}_pre",
                    name=f"pre_adapter_{vertex.label}",
                    wf=self,
                )
                self.nextflow_tasks.append(pre_adapter_task)

                pre_adapter_node = Node(
                    id=f"n{vertex.id}-pre-adapter",
                    metadata=pre_adapter_task.construct_node_metadata(),
                    bindings=sorted(bindings, key=lambda b: b.var),
                    upstream_nodes=upstream_nodes,
                    flyte_entity=pre_adapter_task,
                )
                extra_nodes.append(pre_adapter_node)

                process_task = NextflowProcessTask(
                    inputs={"default": str},
                    outputs={"default": str},
                    id=vertex.id,
                    name=vertex.label,
                    code=vertex.code,
                    wf=self,
                )
                self.nextflow_tasks.append(process_task)

                process_node = Node(
                    id=f"n{vertex.id}",
                    metadata=process_task.construct_node_metadata(),
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
                    flyte_entity=process_task,
                )

                extra_nodes.append(process_node)

                post_adapter_task = NextflowProcessPostAdapterTask(
                    inputs={"default": List[str]},
                    outputs={x.name: List[str] for x in vertex.output_params},
                    id=f"{vertex.id}_post",
                    name=f"post_adapter_{vertex.label}",
                    wf=self,
                )
                self.nextflow_tasks.append(post_adapter_task)

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
                                        node=process_node,
                                        var="default",
                                    ),
                                ).ref
                            ),
                        )
                    ],
                    upstream_nodes=[process_node],
                    flyte_entity=post_adapter_task,
                )

                node_map[vertex.id] = post_adapter_node

            elif vertex.vertex_type == VertexType.operator:
                operator_task = NextflowOperatorTask(
                    inputs=python_inputs,
                    outputs={f"n{vertex.id}": List[str]},
                    name=vertex.label,
                    id=vertex.id,
                    wf=self,
                )

                node = Node(
                    id=f"n{vertex.id}",
                    metadata=operator_task.construct_node_metadata(),
                    bindings=bindings,
                    upstream_nodes=upstream_nodes,
                    flyte_entity=operator_task,
                )

                node_map[vertex.id] = node

            elif vertex.vertex_type == VertexType.origin:
                # generic channel
                ...

            else:
                raise ValueError(f"Unsupported vertex type for {repr(vertex)}")

        meta = metadata.LatchMetadata(
            display_name=name,
            author=metadata.LatchAuthor(name="Nextflow Workflow"),
            parameters={
                k: metadata.LatchParameter(display_name=k) for k in global_inputs.keys()
            },
        )

        docstring = Docstring(f"{name}\n\nSample Description\n\n" + str(meta))

        python_interface = Interface(
            global_inputs,
            global_outputs,
            docstring=docstring,
        )

        literals: Dict[str, Literal] = {}

        return python_interface, literals, list(node_map.values()) + extra_nodes, []

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)


class NextflowTask(PythonAutoContainerTask[Pod]):
    def __init__(
        self,
        inputs: Dict[str, ParameterType],
        outputs: Dict[str, ParameterType],
        id: int,
        name: str,
        wf: NextflowWorkflow,
    ):
        self.id = id
        name = f"{name}_{id}"
        self.wf = wf

        interface = Interface(inputs, outputs, docstring=None)
        self._python_inputs = inputs
        self._python_outputs = outputs

        # TODO
        cores = 4
        mem = 8589 * 1000 * 1000 // 1024 // 1024 // 1024

        super().__init__(
            task_type="sidecar",
            task_type_version=2,
            name=name,
            interface=interface,
            task_config=custom_task(cpu=cores, memory=mem).keywords["task_config"],
            task_resolver=NextflowTaskResolver(),
        )


class NextflowTaskResolver(DefaultTaskResolver):
    @property
    def location(self) -> str:
        return "flytekit.core.python_auto_container.default_task_resolver"

    def loader_args(
        self, settings: SerializationSettings, task: NextflowTask
    ) -> List[str]:
        return ["task-module", "latch_entrypoint", "task-name", task.name]

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

        inputs = container_task.python_interface.inputs.copy()
        outputs = container_task.python_interface.inputs.copy()

        assert len(inputs) <= 1
        assert len(outputs) <= 1

        k, v = next(iter(inputs.items()))
        list_inputs = {k: List[v]}
        k, v = next(iter(outputs.items()))
        print("key: ", k, v)
        list_outputs = {k: List[v]}

        super().__init__(
            name=name,
            interface=Interface(
                inputs=list_inputs, outputs=list_outputs, docstring=None
            ),
            task_type=SdkTaskType.CONTAINER_ARRAY_TASK,
            task_config=None,
            task_resolver=NextflowTaskResolver(),
        )

        # Make sure when I build code for this that it is a map task function
        # so it will index from literals correctly

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
            self.task_resolver.location,
            "--",
            *self.task_resolver.loader_args(settings, self.container_task),
        ]

    @contextmanager
    def prepare_target(self):
        """
        Alters the underlying run_task command to modify it for map task execution and then resets it after.
        """
        self._run_task.set_command_fn(self.get_command)
        try:
            yield
        finally:
            self._run_task.reset_command_fn()

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


class NextflowProcessMappedTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, ParameterType],
        outputs: Dict[str, ParameterType],
        id: int,
        name: str,
        code: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, wf)
        self.code = code
        self.process_name = name

    def get_fn_interface(self):
        res = ""

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: {t.__name__}
                """,
                1,
            ).rstrip()
            for param, t in self._python_interface.inputs.items()
        )

        res += reindent(
            rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @map_task
                @task(cache=True)
                def {self.name}(
                __params__
                ) -> str
                """,
            0,
        ).replace("__params__", params_str)
        return res

    def get_fn_return_stmt(self, remote_output_url: Optional[str] = None):
        return reindent("return default", 1)

    def get_fn_code(
        self,
        remote_output_url: Optional[str] = None,
    ):
        code_block = self.get_fn_interface()

        # TODO
        nextflow_script_path = "/root/workflow.nf"
        run_task_entrypoint = [
            "/root/latch-nextflow",
            "run",
            nextflow_script_path,
        ]

        nextflow_script_path = "/root/workflow.nf"
        code_block += reindent(
            rf"""
            print("\n\n\nRunning nextflow task\n")
            try:
                subprocess.run(
                    [{','.join([repr(x) for x in run_task_entrypoint])}], 
                    env={{
                        "LATCH_TARGET_PROCESS_NAME": "{self.process_name}",
                        "LATCH_INPUT_VALS": default,
                    }},
                    check=True,
                )
            except Exception as e:
                print("\n\n\n[!] Failed\n\n\n")
                raise e

            with open("/root/.latch/outputValues.json") as f:
                 return json.load(f)

            """,
            1,
        )

        code_block += self.get_fn_return_stmt(remote_output_url=remote_output_url)
        return code_block


class NextflowProcessTask(MapContainerTask):
    def __init__(
        self,
        inputs: Dict[str, ParameterType],
        outputs: Dict[str, ParameterType],
        id: int,
        name: str,
        code: str,
        wf: NextflowWorkflow,
    ):
        mapped = NextflowProcessMappedTask(inputs, outputs, id, name, code, wf)
        super().__init__(mapped)


class NextflowProcessPreAdapterTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, ParameterType],
        outputs: Dict[str, ParameterType],
        id: int,
        name: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, wf)
        self.process_name = name

    def get_fn_interface(self):
        res = ""

        print(self._python_inputs)

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: {t.__name__}
                """,
                1,
            ).rstrip()
            for param, t in self._python_inputs.items()
        )

        res += reindent(
            rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @task(cache=True)
                def {self.name}(
                __params__
                ) -> str
                """,
            0,
        ).replace("__params__", params_str)
        return res

    def get_fn_return_stmt(self, remote_output_url: Optional[str] = None):
        return reindent("return default", 1)

    def get_fn_code(
        self,
        remote_output_url: Optional[str] = None,
    ):
        code_block = self.get_fn_interface()

        code_block += reindent(
            rf"""

            # collate values

            """,
            1,
        )

        code_block += self.get_fn_return_stmt(remote_output_url=remote_output_url)
        return code_block


class NextflowProcessPostAdapterTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, ParameterType],
        outputs: Dict[str, ParameterType],
        id: int,
        name: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, wf)
        self.process_name = name

    def get_fn_interface(self):
        res = ""

        outputs_str = "None:"
        if len(self._python_outputs.items()) > 0:
            output_fields = "\n".join(
                reindent(
                    rf"""
                    {param}: {t.__name__}
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

        res += reindent(
            rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @task(cache=True)
                def {self.name}(
                    default: str
                ) -> __outputs__
                """,
            0,
        ).replace("__outputs__", outputs_str)
        return res

    def get_fn_return_stmt(self, remote_output_url: Optional[str] = None):
        results: List[str] = []
        for out_name, out_type in self._python_outputs.items():
            results.append(
                reindent(
                    rf"""
                    {out_name}={out_name}
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

    def get_fn_code(
        self,
        remote_output_url: Optional[str] = None,
    ):
        code_block = self.get_fn_interface()

        code_block += reindent(
            rf"""

            # spread values

            """,
            1,
        )

        code_block += self.get_fn_return_stmt(remote_output_url=remote_output_url)
        return code_block


class NextflowOperatorTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, ParameterType],
        outputs: Dict[str, ParameterType],
        id: int,
        name: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, wf)


def build_nf_wf():
    with open(".latch/nextflowDAG.json") as f:
        dag = json.load(f)

    vertices_json = dag["vertices"]
    edges_json = dag["edges"]

    vertices: Dict[int, NextflowDAGVertex] = {}
    dependent_vertices: Dict[int, List[int]] = {}
    for vertex_json in vertices_json:
        vertex_content = vertex_json["content"]
        code = None
        if "source" in vertex_content:
            code = vertex_content["source"]

        def format_param_name(name: str, t: NextflowParamType):
            if name == "-":
                return "stdout"
            if t in (NextflowInputParamType.path, NextflowOutputParamType.fileoutparam):
                return Path(name).stem
            if t == NextflowOutputParamType.tupleoutparam:
                return name.replace("<", "_").replace(">", "_")
            return name

        input_params = []
        if vertex_content["inputParams"]:
            for x in vertex_content["inputParams"]:
                t = NextflowInputParamType(x["type"])
                input_params.append(
                    NextflowParam(name=format_param_name(x["name"], t), paramType=t)
                )

        output_params = []
        if vertex_content["outputParams"]:
            for x in vertex_content["outputParams"]:
                t = NextflowOutputParamType(x["type"])
                output_params.append(
                    NextflowParam(name=format_param_name(x["name"], t), paramType=t)
                )

        vertex = NextflowDAGVertex(
            id=vertex_content["id"],
            label=vertex_content["label"],
            vertex_type=VertexType(vertex_content["type"].lower()),
            input_params=input_params,
            output_params=output_params,
            code=code,
        )
        vertices[vertex.id] = vertex
        dependent_vertices[vertex.id] = []

    edges: Dict[int, NextflowDAGEdge] = {}
    edges_idx_by_start: Dict[int, NextflowDAGEdge] = {}
    edges_idx_by_end: Dict[int, NextflowDAGEdge] = {}
    for i in vertices.keys():
        edges_idx_by_start[i] = []
        edges_idx_by_end[i] = []

    for edge_json in edges_json:
        edge_content = edge_json["content"]

        edge = NextflowDAGEdge(
            id=edge_content["id"],
            idx=edge_content["idx"],
            label=edge_content["label"],
            connection=edge_content["connection"],
        )
        edges[edge.id] = edge
        if edge.connection[0]:
            edges_idx_by_start[edge.connection[0]].append(edge)

        if edge.connection[1]:
            edges_idx_by_end[edge.connection[1]].append(edge)

        from_vertex, to_vertex = edge.connection
        if to_vertex is not None:
            dependent_vertices[to_vertex].append(from_vertex)

    nf_wf = NextflowWorkflow()
