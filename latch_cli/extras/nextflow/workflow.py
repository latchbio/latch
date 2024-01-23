import importlib
import json
import os
import subprocess
import sys
import textwrap
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union, get_args, get_origin

import click
from flytekit.configuration import SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.constants import SdkTaskType
from flytekit.core.docstring import Docstring
from flytekit.core.interface import Interface, transform_variable_map
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
from flytekitplugins.pod.task import Pod

from latch.resources.tasks import custom_task
from latch.types import metadata
from latch.types.metadata import ParameterType, _IsDataclass
from latch_cli.utils import identifier_from_str

from ..common.serialize import binding_from_python
from ..common.utils import reindent, type_repr
from .types import (
    NextflowDAGEdge,
    NextflowDAGVertex,
    NextflowInputParamType,
    NextflowOutputParamType,
    NextflowParam,
    VertexType,
)
from .utils import format_param_name


class NextflowWorkflow(WorkflowBase, ClassStorageTaskResolver):
    def __init__(
        self,
        vertices: Dict[int, NextflowDAGVertex],
        dependent_vertices: Dict[int, List[int]],
        dependent_edges_by_start: Dict[int, List[NextflowDAGEdge]],
        dependent_edges_by_end: Dict[int, List[NextflowDAGEdge]],
    ):
        # todo(ayush): consolidate w/ snakemake

        assert metadata._nextflow_metadata is not None

        docstring = Docstring(
            f"{metadata._nextflow_metadata.display_name}\n\nSample Description\n\n"
            + str(metadata._nextflow_metadata)
        )
        python_interface = Interface(
            {
                k: (v.type, v.default)
                for k, v in metadata._nextflow_metadata.parameters.items()
                if v.type is not None and v.default is not None
            },
            {},
            docstring=docstring,
        )

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

        self.build_from_nextflow_dag(
            vertices,
            dependent_vertices,
            dependent_edges_by_start,
            dependent_edges_by_end,
            python_interface,
        )

    def build_from_nextflow_dag(
        self,
        vertices: Dict[int, NextflowDAGVertex],
        dependent_vertices: Dict[int, List[int]],
        dependent_edges_by_start: Dict[int, List[NextflowDAGEdge]],
        dependent_edges_by_end: Dict[int, List[NextflowDAGEdge]],
        python_interface: Interface,
    ):
        global_start_node = Node(
            id=_common_constants.GLOBAL_INPUT_NODE_ID,
            metadata=None,
            bindings=[],
            upstream_nodes=[],
            flyte_entity=None,
        )

        interface_inputs = transform_variable_map(python_interface.inputs)

        task_bindings = []
        for k in python_interface.inputs:
            var = interface_inputs[k]
            promise_to_bind = Promise(
                var=k,
                val=NodeOutput(node=global_start_node, var=k),
            )
            task_bindings.append(
                binding_from_python(
                    var_name=k,
                    expected_literal_type=var.type,
                    t_value=promise_to_bind,
                    t_value_type=interface_inputs[k],
                )
            )

        task_interface = Interface(python_interface.inputs, None, docstring=None)
        # todo(ayush): need to add execute method here
        main_task = NextflowMainTask(interface=task_interface)

        main_node = Node(
            id="main",
            metadata=main_task.construct_node_metadata(),
            bindings=sorted(task_bindings, key=lambda x: x.var),
            upstream_nodes=[],
            flyte_entity=main_task,
        )

        node_map: Dict[int, Node] = {}
        extra_nodes: List[Node] = [main_node]
        main_node_outputs: Dict[str, Type[ParameterType]] = {}
        for vertex_id in sorted(dependent_vertices.keys()):
            vertex = vertices[vertex_id]
            if vertex.vertex_type == VertexType.origin:
                continue

            upstream_nodes = []
            bindings: List[literals_models.Binding] = []
            for edge in dependent_edges_by_end[vertex.id]:
                depen_vertex = vertices.get(edge.connection[0])
                param_name = f"c{edge.from_idx}"

                if (
                    depen_vertex is None
                    or depen_vertex.vertex_type == VertexType.origin
                ):
                    if vertex.id not in main_task.main_target_ids:
                        main_task.main_target_ids.append(vertex.id)

                    main_out_param_name = f"v{vertex.id}_c{edge.to_idx}"
                    node_output = NodeOutput(node=main_node, var=main_out_param_name)

                    main_node_outputs[main_out_param_name] = List[str]
                else:
                    source_param = f"c{edge.to_idx}"
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

            python_inputs = {
                f"c{e.from_idx}": List[str]
                for e in dependent_edges_by_end.get(vertex.id, [])
            }
            python_outputs = {
                f"c{e.to_idx}": List[str]
                for e in dependent_edges_by_start.get(vertex.id, [])
            }

            if vertex.vertex_type == VertexType.process:
                pre_adapter_task = NextflowProcessPreAdapterTask(
                    inputs=python_inputs,
                    id=f"{vertex.id}_pre",
                    name=f"pre_adapter_{identifier_from_str(vertex.label)}",
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

                post_adapter_task = NextflowProcessPostAdapterTask(
                    outputs=python_outputs,
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

                    if is_dataclass(d):
                        return d, len(fields(d))

                    return d, 0

                input_dataclass, num_inputs = parse_dataclass(
                    pre_adapter_task._python_outputs["default"]
                )
                output_dataclass, num_outputs = parse_dataclass(
                    post_adapter_task._python_inputs["default"]
                )

                process_task = NextflowProcessTask(
                    inputs={"default": input_dataclass},
                    outputs={"default": output_dataclass},
                    num_inputs=num_inputs,
                    num_outputs=num_outputs,
                    id=vertex.id,
                    name=identifier_from_str(vertex.label),
                    code=vertex.code,
                    wf=self,
                )

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

                # adapter tasks are ordered before process task because of
                # dependent types defined in code generated from adapters

                extra_nodes.append(process_node)
                self.nextflow_tasks.append(process_task)

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
                    outputs=python_outputs,
                    name=vertex.label,
                    id=vertex.id,
                    wf=self,
                )
                self.nextflow_tasks.append(operator_task)

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

        self._nodes = list(node_map.values()) + extra_nodes

        main_task._python_outputs = main_node_outputs
        self.main_task = main_task
        main_node.flyte_entity._interface._outputs = transform_variable_map(
            main_node_outputs
        )

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)


class NextflowTask(PythonAutoContainerTask[Pod]):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        outputs: Dict[str, Type[ParameterType]],
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
            task_type="python-task",
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

        inputs = container_task.python_interface.inputs.copy()
        outputs = container_task.python_interface.outputs.copy()

        assert len(inputs) <= 1
        assert len(outputs) <= 1

        k, v = next(iter(inputs.items()))
        list_inputs = {k: List[v]}
        k, v = next(iter(outputs.items()))
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
        inputs: Dict[str, Type[ParameterType]],
        outputs: Dict[str, Type[ParameterType]],
        num_inputs: int,
        num_outputs: int,
        id: int,
        name: str,
        code: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, wf)

        self.code = code
        self.process_name = name
        assert len(self._python_inputs) == 1 and len(self._python_outputs) == 1

        self.num_inputs = num_inputs
        self.num_outputs = num_outputs

    def get_fn_interface(self):
        res = ""
        params_str = ",\n".join(
            reindent(
                rf"""
                """,
                1,
            ).rstrip()
            for param, t in self._python_interface.inputs.items()
        )

        inputs = list(self._python_inputs.items())[0]
        output_t = list(self._python_outputs.values())[0]
        res += reindent(
            rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @map_task
                @task(cache=True)
                def {self.name}(
                    {inputs[0]}: {getattr(inputs[1], "__name__", None)}
                ) -> {getattr(output_t, "__name__", None)}:
                """,
            0,
        ).replace("__params__", params_str)
        return res

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

        run_task_entrypoint = [".latch/bin/nextflow", "run", nf_path_in_container]

        code_block += reindent(
            rf"""

            channel_vals = [{','.join([f"default.c{i}" for i in range(self.num_inputs)])}]

            print("\n\n\nRunning nextflow task: {run_task_entrypoint}\n")
            try:
                subprocess.run(
                    [{','.join([repr(x) for x in run_task_entrypoint])}],
                    env={{
                        **os.environ,
                        "LATCH_TARGET_PROCESS_NAME": "{self.process_name}",
                        "LATCH_PARAM_VALS": json.dumps(channel_vals),
                    }},
                    check=True,
                )
            except Exception as e:
                print("\n\n\n[!] Failed\n\n\n")
                raise e


            out_channels = {{}}
            vals = Path(".latch/process-out.txt").read_text().strip().split("\n")

            for i, val in enumerate(vals):
                out_channels[f"c{{i}}"] = val
            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block


class NextflowProcessTask(MapContainerTask):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        outputs: Dict[str, Type[ParameterType]],
        num_inputs: int,
        num_outputs: int,
        id: int,
        name: str,
        code: str,
        wf: NextflowWorkflow,
    ):
        mapped = NextflowProcessMappedTask(
            inputs, outputs, num_inputs, num_outputs, id, name, code, wf
        )
        super().__init__(mapped)


def dataclass_code_from_python_params(
    params: Dict[str, Type[ParameterType]], name: str
):
    output_fields = "\n".join(
        reindent(f"{param}: str", 1).rstrip() for param, t in params.items()
    )

    return reindent(
        rf"""
        @dataclass
        class Res_{name}:
        __output_fields__

        """,
        0,
    ).replace("__output_fields__", output_fields)


class NextflowProcessPreAdapterTask(NextflowTask):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        id: int,
        name: str,
        wf: NextflowWorkflow,
    ):
        self.num_params = len(inputs)

        if len(inputs) > 0:
            self.dataclass_code = dataclass_code_from_python_params(inputs, id)
            exec(self.dataclass_code, globals())

            outputs = {"default": List[globals()[f"Res_{id}"]]}
        else:
            self.dataclass_code = ""

            outputs = {"default": List[None]}

        super().__init__(inputs, outputs, id, name, wf)
        self.process_name = name

    def get_fn_interface(self):
        res = ""

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: List[str]
                """,
                1,
            ).rstrip()
            for param, t in self._python_inputs.items()
        )

        res += self.dataclass_code

        output_typ = self._python_outputs["default"]

        res += (
            reindent(
                rf"""
                task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec
                @task(cache=True)
                def {self.name}(
                __params__
                ) -> __outputs__:
                """,
                0,
            )
            .replace("__outputs__", type_repr(output_typ))
            .replace("__params__", params_str)
        )

        return res

    def get_fn_return_stmt(self):
        return reindent("return result", 1)

    def get_fn_code(self, nf_path_in_container: str):
        code_block = self.get_fn_interface()

        assignment_str = ",".join([f"c{i}=x[{i}]" for i in range(self.num_params)])
        variables = ",".join([f"c{i}" for i in range(self.num_params)])

        if len(self._python_inputs) == 0:
            code_block += reindent(
                f"""
                result = None
                """,
                1,
            )
        else:
            code_block += reindent(
                rf"""
                result = [Res_{self.id}({assignment_str}) for x in zip({variables})]
                """,
                1,
            )

        code_block += self.get_fn_return_stmt()
        return code_block


class NextflowProcessPostAdapterTask(NextflowTask):
    def __init__(
        self,
        outputs: Dict[str, Type[ParameterType]],
        id: int,
        name: str,
        wf: NextflowWorkflow,
    ):
        self.dataclass_code = dataclass_code_from_python_params(outputs, id)
        exec(self.dataclass_code, globals())

        inputs = {"default": List[globals()[f"Res_{id}"]]}
        super().__init__(inputs, outputs, id, name, wf)
        self.process_name = name

    def get_fn_interface(self):
        res = ""

        output_fields = "\n".join(
            reindent(
                rf"""
                {param}: List[str]
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
                    default: List[Res_{self.id}]
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
        id: int,
        name: str,
        wf: NextflowWorkflow,
    ):
        self.operator_id = id
        super().__init__(inputs, outputs, id, name, wf)

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
                    {out_name}=out_channels.get("{out_name}", [])
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

        run_task_entrypoint = [
            ".latch/bin/nextflow",
            "run",
            nf_path_in_container,
            "-latchTarget",
        ]

        code_block += reindent(
            rf"""

            channel_vals = [{','.join([x for x in self._python_inputs])}]

            print(f"\n\n\nRunning nextflow task: {run_task_entrypoint}\n")
            try:
                subprocess.run(
                    [{','.join([repr(x) for x in run_task_entrypoint])}],
                    env={{
                        **os.environ,
                        "LATCH_TARGET_OPERATOR_ID": "{self.operator_id}",
                        "LATCH_CHANNEL_VALS": json.dumps(channel_vals),
                    }},
                    check=True,
                )
            except Exception as e:
                print("\n\n\n[!] Failed\n\n\n")
                raise e


            out_channels = {{}}
            files = list(glob.glob(".latch/channel*.txt"))
            for file in files:
                idx = parse_channel_file(file)
                vals = Path(file).read_text().strip().split("\n")
                out_channels[f"c{{idx}}"] = vals
            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block


class NextflowMainTask(PythonAutoContainerTask[Pod]):
    def __init__(self, interface: Interface):
        self._python_inputs = interface.inputs
        self.main_target_ids = []
        super().__init__(
            name="main",
            task_type="python-task",
            interface=interface,
            task_config=None,
            task_resolver=NextflowTaskResolver(),
        )

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
                    {out_name}=out_channels.get("{out_name}", [])
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

        run_task_entrypoint = [
            ".latch/bin/nextflow",
            "run",
            nf_path_in_container,
        ]

        code_block += reindent(
            rf"""
            print(f"\n\n\nRunning nextflow task: {run_task_entrypoint}\n")
            try:
                subprocess.run(
                    [{','.join([repr(x) for x in run_task_entrypoint])}],
                    env={{
                        **os.environ,
                        "LATCH_MAIN_TARGET_IDS": "{json.dumps(self.main_target_ids)}",
                    }},
                    check=True,
                )
            except Exception as e:
                print("\n\n\n[!] Failed\n\n\n")
                raise e


            out_channels = {{}}
            files = list(glob.glob(".latch/*/channel*.txt"))
            for file in files:
                idx = parse_channel_file(file)
                vals = Path(file).read_text().strip().split("\n")
                v_id = Path(file).parent.name
                out_channels[f"v{{v_id}}_c{{idx}}"] = vals
            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block


def build_nf_wf(pkg_root: Path, nf_script: Path):
    try:
        subprocess.run(
            [
                str(pkg_root / ".latch/bin/nextflow"),
                "run",
                str(nf_script),
                "-with-dag",
                "-latchJIT",
                "--input",
                str(pkg_root / "assets" / "samplesheet.csv"),
                "--outdir",
                str(pkg_root),
                "--run_amp_screening",
                "--amp_skip_hmmsearch",
                "--run_arg_screening",
                "--run_bgc_screening",
                "--bgc_skip_hmmsearch",
            ],
            check=True,
        )
    except Exception as e:
        print("\n\n\n[!] Failed\n\n\n")
        raise e

    with open(pkg_root / ".latch/nextflowDAG.json") as f:
        dag = json.load(f)

    vertices_json = dag["vertices"]
    edges_json = dag["edges"]

    vertices: Dict[int, NextflowDAGVertex] = {}
    dependent_vertices: Dict[int, List[int]] = {}
    for v in vertices_json:
        content = v["content"]

        code: Optional[str] = None
        if "source" in content:
            code = content["source"]

        input_params: List[NextflowParam] = []
        if content["inputParams"] is not None:
            for x in content["inputParams"]:
                t = NextflowInputParamType(x["type"])

                input_params.append(
                    NextflowParam(name=format_param_name(x["name"], t), type=t)
                )

        output_params: List[NextflowParam] = []
        if content["outputParams"] is not None:
            for x in content["outputParams"]:
                t = NextflowOutputParamType(x["type"])

                output_params.append(
                    NextflowParam(name=format_param_name(x["name"], t), type=t)
                )

        vertex = NextflowDAGVertex(
            id=content["id"],
            label=content["label"],
            vertex_type=VertexType(content["type"].lower()),
            input_params=input_params,
            output_params=output_params,
            code=code,
        )

        vertices[vertex.id] = vertex
        dependent_vertices[vertex.id] = []

    dependent_edges_by_start: Dict[int, List[NextflowDAGEdge]] = {}
    dependent_edges_by_end: Dict[int, List[NextflowDAGEdge]] = {}
    for i in vertices.keys():
        dependent_edges_by_start[i] = []
        dependent_edges_by_end[i] = []

    for edge_json in edges_json:
        edge_content = edge_json["content"]

        edge = NextflowDAGEdge(
            id=edge_content["id"],
            to_idx=edge_content["outIdx"],
            from_idx=edge_content["inIdx"],
            label=edge_content["label"],
            connection=edge_content["connection"],
        )

        if edge.connection[0] is not None:
            dependent_edges_by_start[edge.connection[0]].append(edge)

        if edge.connection[1] is not None:
            dependent_edges_by_end[edge.connection[1]].append(edge)

        from_vertex, to_vertex = edge.connection
        if to_vertex is not None:
            dependent_vertices[to_vertex].append(from_vertex)

    return NextflowWorkflow(
        vertices, dependent_vertices, dependent_edges_by_start, dependent_edges_by_end
    )


def nf_path_in_container(nf_script: Path, pkg_root: Path) -> str:
    return str(nf_script.resolve())[len(str(pkg_root.resolve())) + 1 :]


def generate_nf_entrypoint(
    wf: NextflowWorkflow,
    pkg_root: Path,
    nf_path: Path,
):
    entrypoint_code_block = textwrap.dedent(r"""
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

        from flytekit.extras.persistence import LatchPersistence
        import traceback

        from latch.resources.tasks import custom_task
        from latch.resources.map_tasks import map_task
        from latch.types.directory import LatchDir, LatchOutputDir
        from latch.types.file import LatchFile

        from latch_cli.utils import get_parameter_json_value, urljoins, check_exists_and_rename

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)

        NoneType = type(None)

        channel_pattern = r"channel(\d+)\.txt"

        def parse_channel_file(fname: str) -> int:
            match = re.search(channel_pattern, Path(fname).name)
            if match:
                return match.group(1)
            raise ValueError(f"Malformed file name for parameter output: {fname}")

    """).lstrip()

    entrypoint_code_block += wf.main_task.get_fn_code(
        nf_path_in_container(nf_path, pkg_root)
    )

    for task in wf.nextflow_tasks:
        if isinstance(task, NextflowProcessTask):
            entrypoint_code_block += (
                task.container_task.get_fn_code(nf_path_in_container(nf_path, pkg_root))
                + "\n\n"
            )
        else:
            entrypoint_code_block += (
                task.get_fn_code(nf_path_in_container(nf_path, pkg_root)) + "\n\n"
            )

    entrypoint = pkg_root / ".latch/nf_entrypoint.py"
    entrypoint.write_text(entrypoint_code_block + "\n")
