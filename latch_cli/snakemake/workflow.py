import importlib
import textwrap
import typing
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

import snakemake
from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
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
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.type_engine import TypeEngine
from flytekit.core.utils import _dnsify
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
from snakemake.executors import RealExecutor
from snakemake.persistence import Persistence
from snakemake.resources import DefaultResources, ResourceScopes, parse_resources
from snakemake.rules import Rule
from snakemake.target_jobs import encode_target_jobs_cli_args
from snakemake.workflow import Workflow

from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter

T = TypeVar("T")


def variable_name_for_target_file(name: str):

    return name.replace("/", "_").replace(".", "_")


def snakemake_dag_to_interface(
    dag: DAG, docstring: Optional[Docstring] = None
) -> Interface:

    outputs: Dict[str, Type] = {}

    for target in dag.targetjobs:
        if type(target.input) == snakemake.io.InputFiles:
            for x in target.input:
                outputs[variable_name_for_target_file(x.file)] = LatchFile
        else:
            raise ValueError(f"Unsupported snakemake input type {type(target.input)}")

    inputs: Dict[str, Type] = {}
    for job in dag.jobs:
        dep_outputs = []
        for dep, dep_files in dag.dependencies[job].items():
            for o in dep.output:
                if o in dep_files:
                    dep_outputs.append(o)

        for x in job.input:
            if x not in dep_outputs:
                inputs[variable_name_for_target_file(x.file)] = (LatchFile, None)

    return Interface(inputs, outputs, docstring=docstring)


def binding_data_from_python(
    expected_literal_type: type_models.LiteralType,
    t_value: typing.Any,
    t_value_type: Optional[type] = None,
) -> literals_models.BindingData:

    if isinstance(t_value, Promise):
        if not t_value.is_ready:
            return literals_models.BindingData(promise=t_value.ref)


def binding_from_python(
    var_name: str,
    expected_literal_type: type_models.LiteralType,
    t_value: typing.Any,
    t_value_type: type,
) -> literals_models.Binding:
    binding_data = binding_data_from_python(
        expected_literal_type, t_value, t_value_type
    )
    return literals_models.Binding(var=var_name, binding=binding_data)


def transform_type(x: type, description: str = None) -> interface_models.Variable:
    return interface_models.Variable(
        type=TypeEngine.to_literal_type(x), description=description
    )


def transform_types_in_variable_map(
    variable_map: Dict[str, type],
    descriptions: Dict[str, str] = {},
) -> Dict[str, interface_models.Variable]:
    res = OrderedDict()
    if variable_map:
        for k, v in variable_map.items():
            res[k] = transform_type(v, descriptions.get(k, k))
    return res


def interface_to_parameters(
    interface: Interface,
) -> interface_models.ParameterMap:
    if interface is None or interface.inputs_with_defaults is None:
        return interface_models.ParameterMap({})
    if interface.docstring is None:
        inputs_vars = transform_types_in_variable_map(interface.inputs)
    else:
        inputs_vars = transform_types_in_variable_map(
            interface.inputs, interface.docstring.input_descriptions
        )
    params = {}
    inputs_with_def = interface.inputs_with_defaults
    for k, v in inputs_vars.items():
        val, _default = inputs_with_def[k]
        required = _default is None
        default_lv = None
        if _default is not None:
            default_lv = TypeEngine.to_literal(
                None, _default, python_type=interface.inputs[k], expected=v.type
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

        parameter_metadata = {}
        for job in dag.jobs:
            for x in job.input:
                var = variable_name_for_target_file(x.file)
                parameter_metadata[var] = LatchParameter(display_name=var)

        latch_metadata = LatchMetadata(
            display_name=name,
            documentation="",
            author=LatchAuthor(
                name="",
                email="",
                github="",
            ),
            repository="",
            license="",
            parameters=parameter_metadata,
            tags=[],
        )
        docstring = Docstring(f"{name}\n\nSample Description\n\n" + str(latch_metadata))

        native_interface = snakemake_dag_to_interface(dag, docstring)

        self._input_parameters = None
        self._dag = dag

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

        entrypoint_code_block = textwrap.dedent(
            """\
               import subprocess
               from pathlib import Path
               from typing import NamedTuple

               from latch import small_task
               from latch.types import LatchFile

               def ensure_parents_exist(path: Path):
                   path.parent.mkdir(parents=True, exist_ok=True)
                   return path
               """
        )

        node_map = {}

        target_files = [x for job in self._dag.targetjobs for x in job.input]

        for layer in self._dag.toposorted():
            for job in layer:

                is_target = False

                if job in self._dag.targetjobs:
                    continue

                target_file_for_param: Dict[str, str] = {}

                python_outputs: Dict[str, Type] = {}
                for x in job.output:
                    if x.file in target_files:
                        is_target = True
                    param = variable_name_for_target_file(x.file)
                    target_file_for_param[param] = x.file
                    python_outputs[param] = LatchFile

                dep_outputs = {}
                for dep, dep_files in self._dag.dependencies[job].items():
                    for o in dep.output:
                        if o in dep_files:
                            dep_outputs[o] = dep.jobid

                python_inputs: Dict[str, Type] = {}
                promise_map: Dict[str, str] = {}
                for x in job.input:
                    param = variable_name_for_target_file(x.file)
                    target_file_for_param[param] = x.file
                    python_inputs[param] = LatchFile
                    if x in dep_outputs:
                        promise_map[param] = dep_outputs[x]

                interface = Interface(python_inputs, python_outputs, docstring=None)
                task = SnakemakeJobTask(
                    job=job,
                    inputs=python_inputs,
                    outputs=python_outputs,
                    target_file_for_param=target_file_for_param,
                    is_target=is_target,
                    interface=interface,
                )
                entrypoint_code_block += task.get_fn_code()

                typed_interface = transform_interface_to_typed_interface(interface)

                self.interface.inputs.keys()
                bindings = []
                for k in sorted(interface.inputs):

                    var = typed_interface.inputs[k]
                    if var.description in promise_map:
                        promise_to_bind = Promise(
                            var=k,
                            val=NodeOutput(
                                node=node_map[promise_map[var.description]],
                                var=var.description,
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

        bindings = []
        output_names = list(self.interface.outputs.keys())
        for i, out in enumerate(output_names):

            def find_upstream_node():
                for j in self._dag.targetjobs:
                    for depen, files in self._dag.dependencies[j].items():
                        for f in files:
                            if variable_name_for_target_file(f) == out:
                                return depen.jobid, variable_name_for_target_file(f)

            upstream_id, upstream_var = find_upstream_node()
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

        with open("latch_entrypoint.py", "w") as f:
            f.write(entrypoint_code_block)

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)


class SnakemakeJobTask(PythonAutoContainerTask[T]):
    def __init__(
        self,
        job: snakemake.jobs.Job,
        inputs: Dict[str, Type],
        outputs: Dict[str, Type],
        target_file_for_param: Dict[str, str],
        is_target: bool,
        interface: Interface,
        task_type="python-task",
    ):

        name = f"{job.name}_{job.jobid}"

        self.job = job
        self._is_target = is_target
        self._python_inputs = inputs
        self._python_outputs = outputs
        self._target_file_for_param = target_file_for_param

        def placeholder():
            ...

        self._task_function = placeholder

        super().__init__(
            task_type=task_type,
            name=name,
            interface=interface,
            task_config=None,
            task_resolver=SnakemakeJobTaskResolver(),
        )

    def get_fn_code(self, wf):

        code_block = ""

        fn_interface = f"\n\n@small_task\ndef {self.name}("
        for idx, (param, t) in enumerate(self._python_inputs.items()):
            fn_interface += f"{param}: {t.__name__}"
            if idx == len(self._python_inputs) - 1:
                fn_interface += ")"
            else:
                fn_interface += ", "

        if len(self._python_outputs.items()) > 0:
            for idx, (param, t) in enumerate(self._python_outputs.items()):
                if idx == 0:
                    fn_interface += f" -> NamedTuple('{self.name}_output', "
                fn_interface += f"{param}={t.__name__}"
                if idx == len(self._python_outputs) - 1:
                    fn_interface += "):"
                else:
                    fn_interface += ", "
        else:
            fn_interface += ":"

        code_block += fn_interface

        for param, t in self._python_inputs.items():
            if t == LatchFile:
                code_block += f'\n\tPath({param}).resolve().rename(ensure_parents_exist(Path("{self._target_file_for_param[param]}")))'

        executor = RealExecutor(wf, self._dag)
        executor.cores = 8
        snakemake_cmd = ["snakemake", *executor.get_job_args(self.job).split(" ")]
        snakemake_cmd.remove("")
        formatted_snakemake_cmd = "\n\n\tsubprocess.run(["

        for i, arg in enumerate(snakemake_cmd):
            arg_wo_quotes = arg.strip('"').strip("'")
            formatted_snakemake_cmd += f'"{arg_wo_quotes}"'
            if i == len(snakemake_cmd) - 1:
                formatted_snakemake_cmd += "], check=True)"
            else:
                formatted_snakemake_cmd += ", "
        code_block += formatted_snakemake_cmd

        return_stmt = "\n\treturn ("
        for i, x in enumerate(self._python_outputs):
            if self._is_target:
                return_stmt += (
                    f"LatchFile('{self._target_file_for_param[x]}',"
                    f" 'latch:///{self._target_file_for_param[x]}')"
                )
            else:
                return_stmt += f"LatchFile('{self._target_file_for_param[x]}')"
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
        """
        This method will be invoked to execute the task. If you do decide to override this method you must also
        handle dynamic tasks or you will no longer be able to use the task as a dynamic task generator.
        """
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
