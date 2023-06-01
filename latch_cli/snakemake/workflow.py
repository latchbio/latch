import importlib
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, TypeAlias, TypeVar, Union

import snakemake
from flytekit.configuration import SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.docstring import Docstring
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise
from flytekit.core.python_auto_container import (
    DefaultTaskResolver,
    PythonAutoContainerTask,
)
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import (
    WorkflowBase,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models import interface as interface_models
from flytekit.models import literals as literals_models
from flytekit.models import types as type_models
from snakemake.dag import DAG
from snakemake.target_jobs import encode_target_jobs_cli_args

from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter

SnakemakeInputVal: TypeAlias = snakemake.io._IOFile


T = TypeVar("T")


@dataclass
class JobOutputInfo:
    jobid: str
    output_param_name: str


def task_fn_placeholder():
    ...


def variable_name_for_file(file: snakemake.io.AnnotatedString):
    return file.replace("/", "_").replace(".", "__").replace("-", "____")


def variable_name_for_value(
    val: SnakemakeInputVal,
    params: Union[snakemake.io.InputFiles, snakemake.io.OutputFiles, None] = None,
) -> str:
    if params is not None:
        for name, v in params.items():
            if val == v:
                return name

    return variable_name_for_file(val.file)


def snakemake_dag_to_interface(
    dag: DAG, docstring: Optional[Docstring] = None
) -> Interface:
    outputs: Dict[str, LatchFile] = {}

    for target in dag.targetjobs:
        for x in target.input:
            outputs[variable_name_for_value(x, target.input)] = LatchFile

    inputs: Dict[str, Tuple[LatchFile, None]] = {}
    for job in dag.jobs:
        dep_outputs = []
        for dep, dep_files in dag.dependencies[job].items():
            for o in dep.output:
                if o in dep_files:
                    dep_outputs.append(o)

        for x in job.input:
            if x not in dep_outputs:
                inputs[variable_name_for_value(x, job.input)] = (
                    LatchFile,
                    None,
                )

    return Interface(inputs, outputs, docstring=docstring)


def binding_data_from_python(
    expected_literal_type: type_models.LiteralType,
    t_value: typing.Any,
    t_value_type: Optional[Type] = None,
) -> Optional[literals_models.BindingData]:
    if isinstance(t_value, Promise):
        if not t_value.is_ready:
            return literals_models.BindingData(promise=t_value.ref)


def binding_from_python(
    var_name: str,
    expected_literal_type: type_models.LiteralType,
    t_value: typing.Any,
    t_value_type: Type,
) -> literals_models.Binding:
    binding_data = binding_data_from_python(
        expected_literal_type, t_value, t_value_type
    )
    return literals_models.Binding(var=var_name, binding=binding_data)


def transform_type(
    x: Type, description: Optional[str] = None
) -> interface_models.Variable:
    return interface_models.Variable(
        type=TypeEngine.to_literal_type(x), description=description
    )


def transform_types_in_variable_map(
    variable_map: Dict[str, Type],
    descriptions: Dict[str, str] = {},
) -> Dict[str, interface_models.Variable]:
    res = {}
    if variable_map:
        for k, v in variable_map.items():
            res[k] = transform_type(v, descriptions.get(k, k))
    return res


def interface_to_parameters(
    interface: Optional[Interface],
) -> interface_models.ParameterMap:
    if interface is None or interface.inputs_with_defaults is None:
        return interface_models.ParameterMap({})
    if interface.docstring is None:
        inputs_vars = transform_types_in_variable_map(interface.inputs)
    else:
        inputs_vars = transform_types_in_variable_map(
            interface.inputs, interface.docstring.input_descriptions
        )
    params: Dict[str, interface_models.ParameterMap] = {}
    for k, v in inputs_vars.items():
        val, default = interface.inputs_with_defaults[k]
        required = default is None
        default_lv = None
        if default is not None:
            default_lv = TypeEngine.to_literal(
                None, default, python_type=interface.inputs[k], expected=v.type
            )
        params[k] = interface_models.Parameter(
            var=v, default=default_lv, required=required
        )
    return interface_models.ParameterMap(params)


class SnakemakeWorkflow(WorkflowBase, ClassStorageTaskResolver):
    def __init__(
        self,
        name: str,
        dag: DAG,
    ):
        parameter_metadata: Dict[str, LatchParameter] = {}
        for job in dag.jobs:
            for x in job.input:
                var = variable_name_for_value(x, job.input)
                parameter_metadata[var] = LatchParameter(display_name=var)

        # TODO - support for metadata + parameters in future releases
        latch_metadata = LatchMetadata(
            display_name=name,
            documentation="",
            author=LatchAuthor(
                name="",
                email="",
                github="",
            ),
            parameters=parameter_metadata,
            tags=[],
        )
        docstring = Docstring(f"{name}\n\nSample Description\n\n" + str(latch_metadata))

        native_interface = snakemake_dag_to_interface(dag, docstring)

        self._input_parameters = None
        self._dag = dag
        self.snakemake_tasks = []

        workflow_metadata = WorkflowMetadata(
            on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY
        )
        workflow_metadata_defaults = WorkflowMetadataDefaults(False)
        super().__init__(
            name=name,
            workflow_metadata=workflow_metadata,
            workflow_metadata_defaults=workflow_metadata_defaults,
            python_interface=native_interface,
        )

    def compile(self, **kwargs):
        self._input_parameters = interface_to_parameters(self.python_interface)

        GLOBAL_START_NODE = Node(
            id=_common_constants.GLOBAL_INPUT_NODE_ID,
            metadata=None,
            bindings=[],
            upstream_nodes=[],
            flyte_entity=None,
        )

        node_map: Dict[int, Node] = {}

        target_files = [x for job in self._dag.targetjobs for x in job.input]

        for layer in self._dag.toposorted():
            for job in layer:
                is_target = False

                if job in self._dag.targetjobs:
                    continue

                target_file_for_output_param: Dict[str, str] = {}
                target_file_for_input_param: Dict[str, str] = {}

                python_outputs: Dict[str, LatchFile] = {}
                for x in job.output:
                    if x in target_files:
                        is_target = True
                    param = variable_name_for_value(x, job.output)
                    target_file_for_output_param[param] = x
                    python_outputs[param] = LatchFile

                dep_outputs = {}
                for dep, dep_files in self._dag.dependencies[job].items():
                    for o in dep.output:
                        if o in dep_files:
                            dep_outputs[o] = JobOutputInfo(
                                jobid=dep.jobid,
                                output_param_name=variable_name_for_value(
                                    o, dep.output
                                ),
                            )

                python_inputs: Dict[str, LatchFile] = {}
                promise_map: Dict[str, str] = {}
                for x in job.input:
                    param = variable_name_for_value(x, job.input)
                    target_file_for_input_param[param] = x
                    python_inputs[param] = LatchFile
                    if x in dep_outputs:
                        promise_map[param] = dep_outputs[x]

                interface = Interface(python_inputs, python_outputs, docstring=None)
                task = SnakemakeJobTask(
                    job=job,
                    inputs=python_inputs,
                    outputs=python_outputs,
                    target_file_for_input_param=target_file_for_input_param,
                    target_file_for_output_param=target_file_for_output_param,
                    is_target=is_target,
                    interface=interface,
                )
                self.snakemake_tasks.append(task)

                typed_interface = transform_interface_to_typed_interface(interface)
                bindings: List[literals_models.Binding] = []
                for k in interface.inputs:
                    var = typed_interface.inputs[k]
                    if var.description in promise_map:
                        job_output_info = promise_map[var.description]
                        promise_to_bind = Promise(
                            var=k,
                            val=NodeOutput(
                                node=node_map[job_output_info.jobid],
                                var=job_output_info.output_param_name,
                            ),
                        )
                    else:
                        promise_to_bind = Promise(
                            var=k,
                            val=NodeOutput(node=GLOBAL_START_NODE, var=k),
                        )
                    bindings.append(
                        binding_from_python(
                            var_name=k,
                            expected_literal_type=var.type,
                            t_value=promise_to_bind,
                            t_value_type=interface.inputs[k],
                        )
                    )

                upstream_nodes = []
                for x in self._dag.dependencies[job].keys():
                    if x.jobid in node_map:
                        upstream_nodes.append(node_map[x.jobid])

                node = Node(
                    id=str(job.jobid),
                    metadata=task.construct_node_metadata(),
                    bindings=sorted(bindings, key=lambda b: b.var),
                    upstream_nodes=upstream_nodes,
                    flyte_entity=task,
                )
                node_map[job.jobid] = node

        bindings: List[literals_models.Binding] = []
        for i, out in enumerate(self.interface.outputs.keys()):
            upstream_id, upstream_var = self.find_upstream_node_matching_output_var(out)
            promise_to_bind = Promise(
                var=out,
                val=NodeOutput(node=node_map[upstream_id], var=upstream_var),
            )
            t = self.python_interface.outputs[out]
            b = binding_from_python(
                out,
                self.interface.outputs[out].type,
                promise_to_bind,
                t,
            )
            bindings.append(b)

        self._nodes = list(node_map.values())
        self._output_bindings = bindings

    def find_upstream_node_matching_output_var(self, out_var: str):
        for j in self._dag.targetjobs:
            for depen, files in self._dag.dependencies[j].items():
                for f in files:
                    if variable_name_for_file(f) == out_var:
                        return depen.jobid, variable_name_for_value(f, depen.output)

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)


class SnakemakeJobTask(PythonAutoContainerTask[T]):
    def __init__(
        self,
        job: snakemake.jobs.Job,
        inputs: Dict[str, LatchFile],
        outputs: Dict[str, LatchFile],
        target_file_for_input_param: Dict[str, str],
        target_file_for_output_param: Dict[str, str],
        is_target: bool,
        interface: Interface,
        task_type="python-task",
    ):
        name = f"{job.name}_{job.jobid}"

        self.job = job
        self._is_target = is_target
        self._python_inputs = inputs
        self._python_outputs = outputs
        self._target_file_for_input_param = target_file_for_input_param
        self._target_file_for_output_param = target_file_for_output_param

        self._task_function = task_fn_placeholder

        super().__init__(
            task_type=task_type,
            name=name,
            interface=interface,
            task_config=None,
            task_resolver=SnakemakeJobTaskResolver(),
        )

    def get_fn_code(self, snakefile_path_in_container: str):
        code_block = ""

        fn_interface = f"\n\n@small_task\ndef {self.name}("
        fn_interface += (
            ", ".join(
                f"{param}: {t.__name__}" for param, t in self._python_inputs.items()
            )
            + ")"
        )

        if len(self._python_outputs.items()) > 0:
            fn_interface += (
                f" -> NamedTuple('{self.name}_output', "
                + ", ".join(
                    f"{param}={t.__name__}" for param, t in self._python_outputs.items()
                )
                + "):"
            )
        else:
            fn_interface += ":"

        code_block += fn_interface

        for param, t in self._python_inputs.items():
            if t == LatchFile:
                code_block += f'\n\tPath({param}).resolve().rename(check_exists_and_ensure_parents(Path("{self._target_file_for_input_param[param]}")))'

        snakemake_cmd = [
            "snakemake",
            "-s",
            snakefile_path_in_container,
            "--target-jobs",
            *encode_target_jobs_cli_args(self.job.get_target_spec()),
            "--allowed-rules",
            *self.job.rules,
            "--allowed-rules",
            *self.job.rules,
            "--local-groupid",
            str(self.job.jobid),
            "--cores",
            str(self.job.threads),
        ]
        if not self.job.is_group():
            snakemake_cmd.append("--force-use-threads")

        excluded = {"_nodes", "_cores", "tmpdir"}
        allowed_resources = list(
            filter(lambda x: x[0] not in excluded, self.job.resources.items())
        )
        if len(allowed_resources) > 0:
            snakemake_cmd.append("--resources")
            for resource, value in allowed_resources:
                snakemake_cmd.append(f"{resource}={value}")

        code_block += f"\n\n\tsubprocess.run({repr(snakemake_cmd)}, check=True)"

        return_stmt = "\n\treturn ("
        for i, x in enumerate(self._python_outputs):
            if self._is_target:
                return_stmt += (
                    f"LatchFile('{self._target_file_for_output_param[x]}',"
                    f" 'latch:///{self._target_file_for_output_param[x]}')"
                )
            else:
                return_stmt += f"LatchFile('{self._target_file_for_output_param[x]}')"
            if i == len(self._python_outputs) - 1:
                return_stmt += ")"
            else:
                return_stmt += ", "
        code_block += return_stmt
        return code_block

    @property
    def dockerfile_path(self) -> Path:
        return self._dockerfile_path

    @property
    def task_function(self):
        return self._task_function

    def execute(self, **kwargs) -> Any:
        return exception_scopes.user_entry_point(self._task_function)(**kwargs)


class SnakemakeJobTaskResolver(DefaultTaskResolver):
    @property
    def location(self) -> str:
        return "flytekit.core.python_auto_container.default_task_resolver"

    def loader_args(
        self, settings: SerializationSettings, task: SnakemakeJobTask
    ) -> List[str]:
        return ["task-module", "latch_entrypoint", "task-name", task.name]

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        _, task_module, _, task_name, *_ = loader_args

        task_module = importlib.import_module(task_module)

        task_def = getattr(task_module, task_name)
        return task_def
