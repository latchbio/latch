import importlib
import json
import sys
import textwrap
import typing
from dataclasses import dataclass, is_dataclass
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from urllib.parse import urlparse

import snakemake
import snakemake.io
import snakemake.jobs
from flytekit.configuration import SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
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
from flytekit.models import task as _task_models
from flytekit.models import types as type_models
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralMap, Scalar
from flytekitplugins.pod.task import (
    _PRIMARY_CONTAINER_NAME_FIELD,
    Pod,
    _sanitize_resource_name,
)
from kubernetes.client import ApiClient
from kubernetes.client.models import V1Container, V1EnvVar, V1ResourceRequirements
from snakemake.dag import DAG
from snakemake.jobs import GroupJob, Job
from typing_extensions import TypeAlias, TypedDict

import latch.types.metadata as metadata
from latch.resources.tasks import custom_task
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.snakemake.config.utils import is_primitive_type, type_repr

from ..utils import identifier_suffix_from_str

SnakemakeInputVal: TypeAlias = snakemake.io._IOFile


# old snakemake did not have encode_target_jobs_cli_args
def jobs_cli_args(
    jobs: Iterable[Job],
) -> Generator[str, None, None]:
    for x in jobs:
        wildcards = ",".join(
            f"{key}={value}" for key, value in x.wildcards_dict.items()
        )
        yield f"{x.rule.name}:{wildcards}"


T = TypeVar("T")


# todo(maximsmol): use a stateful writer that keeps track of indent level
def reindent(x: str, level: int) -> str:
    if x[0] == "\n":
        x = x[1:]
    return textwrap.indent(textwrap.dedent(x), "    " * level)


@dataclass
class JobOutputInfo:
    jobid: str
    output_param_name: str
    type_: Union[Type[LatchFile], Type[LatchDir]]


def task_fn_placeholder(): ...


def variable_name_for_file(file: snakemake.io.AnnotatedString):
    if file[0] == "/":
        return f"a_{identifier_suffix_from_str(file)}"

    return f"r_{identifier_suffix_from_str(file)}"


def variable_name_for_value(
    val: SnakemakeInputVal,
    params: Union[snakemake.io.InputFiles, snakemake.io.OutputFiles, None] = None,
) -> str:
    if params is not None:
        for name, v in params.items():
            if val == v:
                return name

    return variable_name_for_file(val.file)


@dataclass
class RemoteFile:
    local_path: str
    remote_path: str


def snakemake_dag_to_interface(
    dag: DAG,
    wf_name: str,
    docstring: Optional[Docstring] = None,
    local_to_remote_path_mapping: Optional[Dict[str, str]] = None,
) -> Tuple[Interface, LiteralMap, List[RemoteFile]]:
    outputs: Dict[str, Union[Type[LatchFile], Type[LatchDir]]] = {}
    for target in dag.targetjobs:
        for desired in target.input:
            param = variable_name_for_value(desired, target.input)

            jobs: List[snakemake.jobs.Job] = dag.file2jobs(desired)
            producer_out: snakemake.io._IOFile = next(x for x in jobs[0].output)
            if producer_out.is_directory:
                outputs[param] = LatchDir
            else:
                outputs[param] = LatchFile

    literals: Dict[str, Literal] = {}
    inputs: Dict[str, Tuple[Type[LatchFile], None]] = {}
    return_files: List[RemoteFile] = []
    for job in dag.jobs:
        dep_outputs = []
        for dep, dep_files in dag.dependencies[job].items():
            for o in dep.output:
                if o in dep_files:
                    dep_outputs.append(o)
            for o in dep.log:
                if o in dep_files:
                    dep_outputs.append(o)

        for x in job.input:
            if x not in dep_outputs:
                param = variable_name_for_value(x, job.input)
                inputs[param] = (LatchFile, None)

                remote_path = (
                    Path("/.snakemake_latch") / "workflows" / wf_name / "inputs" / x
                )
                use_original_remote_path: bool = (
                    local_to_remote_path_mapping is not None
                    and x in local_to_remote_path_mapping
                )

                if use_original_remote_path:
                    remote_path = local_to_remote_path_mapping.get(x)

                remote_url = (
                    urlparse(str(remote_path))._replace(scheme="latch").geturl()
                )

                if not use_original_remote_path:
                    return_files.append(
                        RemoteFile(local_path=x, remote_path=remote_url)
                    )

                literals[param] = Literal(
                    scalar=Scalar(
                        blob=Blob(
                            metadata=BlobMetadata(
                                type=BlobType(
                                    format="",
                                    dimensionality=BlobType.BlobDimensionality.SINGLE,
                                )
                            ),
                            uri=remote_url,
                        ),
                    )
                )

    meta = metadata.LatchMetadata(
        display_name=wf_name,
        author=metadata.LatchAuthor(name="Latch Snakemake JIT"),
        parameters={k: metadata.LatchParameter(display_name=k) for k in inputs.keys()},
    )

    return (
        Interface(
            inputs,
            outputs,
            docstring=Docstring(f"{wf_name}\n\nSample Description\n\n" + str(meta)),
        ),
        LiteralMap(literals=literals),
        return_files,
    )


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

    params: Dict[str, interface_models.Parameter] = {}
    for k, v in inputs_vars.items():
        val, default = interface.inputs_with_defaults[k]
        required = default is None
        default_lv = None

        ctx = FlyteContextManager.current_context()
        if default is not None:
            default_lv = TypeEngine.to_literal(
                ctx, default, python_type=interface.inputs[k], expected=v.type
            )

        params[k] = interface_models.Parameter(
            var=v, default=default_lv, required=required
        )
    return interface_models.ParameterMap(params)


class JITRegisterWorkflow(WorkflowBase, ClassStorageTaskResolver):
    out_parameter_name = "o0"  # must be "o0"

    def __init__(
        self,
    ):
        assert metadata._snakemake_metadata is not None

        parameter_metadata = metadata._snakemake_metadata.parameters
        display_name = metadata._snakemake_metadata.display_name
        name = metadata._snakemake_metadata.name

        docstring = Docstring(
            f"{display_name}\n\nSample Description\n\n"
            + str(metadata._snakemake_metadata)
        )
        python_interface = Interface(
            {k: (v.type, v.default) for k, v in parameter_metadata.items()},
            {self.out_parameter_name: bool},
            docstring=docstring,
        )
        self.parameter_metadata = parameter_metadata
        if metadata._snakemake_metadata.output_dir is not None:
            self.remote_output_url = metadata._snakemake_metadata.output_dir.remote_path
        else:
            self.remote_output_url = None

        workflow_metadata = WorkflowMetadata(
            on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY
        )
        name = f"{name}_jit_register"
        workflow_metadata_defaults = WorkflowMetadataDefaults(False)
        super().__init__(
            name=name,
            workflow_metadata=workflow_metadata,
            workflow_metadata_defaults=workflow_metadata_defaults,
            python_interface=python_interface,
        )

    def get_fn_interface(
        self, decorator_name="small_task", fn_name: Optional[str] = None
    ):
        if fn_name is None:
            fn_name = self.name

        params: List[str] = []
        for param, t in self.python_interface.inputs.items():
            params.append(
                reindent(
                    rf"""
                    {param}: {type_repr(t, add_namespace=True)}
                    """,
                    1,
                ).rstrip()
            )

        params_str = ",\n".join(params)

        return reindent(
            rf"""
            @{decorator_name}
            def {fn_name}(
            __params__
            ) -> bool:
            """,
            0,
        ).replace("__params__", params_str)

    def get_fn_return_stmt(self):
        return reindent(
            rf"""
            return True
            """,
            1,
        )

    def get_fn_code(
        self,
        snakefile_path: str,
        version: str,
        image_name: str,
        account_id: str,
        remote_output_url: Optional[str],
    ):
        task_name = f"{self.name}_task"

        code_block = self.get_fn_interface(fn_name=task_name)

        code_block += reindent(
            r"""
            non_blob_parameters = {}
            local_to_remote_path_mapping = {}
            """,
            1,
        )

        for param, t in self.python_interface.inputs.items():
            param_meta = self.parameter_metadata[param]

            if t in (LatchFile, LatchDir):
                assert isinstance(param_meta, metadata.SnakemakeFileParameter)

                touch_str = f"{param}._create_imposters()"
                if param_meta.download:
                    touch_str = (
                        f'print(f"Downloading {param}: {{{param}.remote_path}}");'
                        f" Path({param}).resolve()"
                    )

                code_block += reindent(
                    rf"""
                    {param}_dst_p = Path("{param_meta.path}")

                    {touch_str}
                    {param}_p = Path({param}.path)
                    print(f"  {{file_name_and_size({param}_p)}}")

                    """,
                    1,
                )

                if t is LatchDir:
                    code_block += reindent(
                        rf"""
                        for x in {param}_p.iterdir():
                            print(f"    {{file_name_and_size(x)}}")

                        """,
                        1,
                    )

                code_block += reindent(
                    rf"""
                    print(f"Moving {param} to {{{param}_dst_p}}")
                    check_exists_and_rename(
                        {param}_p,
                        {param}_dst_p
                    )

                    update_mapping({param}_dst_p, {param}.remote_path, local_to_remote_path_mapping)

                    """,
                    1,
                )

            if not getattr(param_meta, "config", True):
                continue

            val_str = f"get_parameter_json_value({param})"
            if hasattr(param_meta, "path"):
                val_str = repr(str(param_meta.path))

            code_block += reindent(
                rf"""
                print(f"Saving parameter value {param} = {{{val_str}}}")
                non_blob_parameters[{repr(param)}] = {val_str}

                """,
                1,
            )

        code_block += reindent(
            rf"""
            image_name = "{image_name}"
            account_id = "{account_id}"
            snakefile = Path("{snakefile_path}")

            lp = LatchPersistence()
            """,
            1,
        )

        code_block += reindent(
            r"""
            pkg_root = Path(".")

            exec_id_hash = hashlib.sha1()
            token = os.environ["FLYTE_INTERNAL_EXECUTION_ID"]
            exec_id_hash.update(token.encode("utf-8"))
            version = exec_id_hash.hexdigest()[:16]

            jit_wf_version = os.environ["FLYTE_INTERNAL_TASK_VERSION"]
            jit_exec_display_name = execute(
                gql.gql('''
                query executionCreatorsByToken($token: String!) {
                  executionCreatorByToken(token: $token) {
                      flytedbId
                        info {
                        displayName
                      }
                  }
                }
                '''),
                {"token": token},
            )["executionCreatorByToken"]["info"]["displayName"]
            """,
            1,
        )

        code_block += reindent(
            rf"""
            print(f"JIT Workflow Version: {{jit_wf_version}}")
            print(f"JIT Execution Display Name: {{jit_exec_display_name}}")

            wf = extract_snakemake_workflow(pkg_root, snakefile, jit_wf_version, jit_exec_display_name, local_to_remote_path_mapping, non_blob_parameters)
            wf_name = wf.name
            generate_snakemake_entrypoint(wf, pkg_root, snakefile, {repr(remote_output_url)}, non_blob_parameters)

            entrypoint_remote = f"latch:///.snakemake_latch/workflows/{{wf_name}}/{{jit_wf_version}}/{{jit_exec_display_name}}/entrypoint.py"
            lp.upload("latch_entrypoint.py", entrypoint_remote)
            print(f"latch_entrypoint.py -> {{entrypoint_remote}}")
            """,
            1,
        )

        code_block += reindent(
            r"""
            headers = {
                "Authorization": f"Latch-Execution-Token {token}",
            }

            temp_dir = tempfile.TemporaryDirectory()
            with Path(temp_dir.name).resolve() as td:
                serialize_snakemake(wf, td, image_name, config.dkr_repo)

                protos = _recursive_list(td)
                reg_resp = register_serialized_pkg(protos, None, version, account_id)
                # _print_reg_resp(reg_resp, image_name, silent=True)

            wf_spec_remote = f"latch:///.snakemake_latch/workflows/{wf_name}/{version}/spec"
            spec_dir = Path("spec")
            for x_dir in spec_dir.iterdir():
                if not x_dir.is_dir():
                    dst = f"{wf_spec_remote}/{x_dir.name}"
                    print(f"{x_dir} -> {dst}")
                    lp.upload(str(x_dir), dst)
                    print("  done")
                    continue

                for x in x_dir.iterdir():
                    dst = f"{wf_spec_remote}/{x_dir.name}/{x.name}"
                    print(f"{x} -> {dst}")
                    lp.upload(str(x), dst)
                    print("  done")

            class _WorkflowInfoNode(TypedDict):
                id: str


            nodes: Optional[List[_WorkflowInfoNode]] = None
            while True:
                time.sleep(1)
                print("Getting Workflow Data:", end=" ")
                nodes = execute(
                    gql.gql('''
                    query workflowQuery($name: String, $ownerId: BigInt, $version: String) {
                    workflowInfos(condition: { name: $name, ownerId: $ownerId, version: $version}) {
                        nodes {
                            id
                        }
                    }
                    }
                    '''),
                    {"name": wf_name, "version": version, "ownerId": account_id},
                )["workflowInfos"]["nodes"]

                if not nodes:
                    print("Failed. Trying again.")
                else:
                    print("Succeeded.")
                    break


            if len(nodes) > 1:
                raise ValueError(
                    "Invariant violated - more than one workflow identified for unique combination"
                    " of {wf_name}, {version}, {account_id}"
                )

            print(nodes)

            for file in wf.return_files:
                print(f"Uploading {file.local_path} -> {file.remote_path}")
                lp.upload(file.local_path, file.remote_path)

            wf_id = nodes[0]["id"]
            params = gpjson.MessageToDict(wf.literal_map.to_flyte_idl()).get("literals", {})

            print(params)

            _interface_request = {
                "workflow_id": wf_id,
                "params": params,
                "snakemake_jit": True,
            }

            response = requests.post(urljoin(config.nucleus_url, "/api/create-execution"), headers=headers, json=_interface_request)
            print(response.json())
            """,
            1,
        )
        code_block += self.get_fn_return_stmt()
        return code_block


class SnakemakeWorkflow(WorkflowBase, ClassStorageTaskResolver):
    def __init__(
        self,
        dag: DAG,
        jit_wf_version: str,
        jit_exec_display_name: str,
        local_to_remote_path_mapping: Optional[Dict[str, str]] = None,
    ):
        assert metadata._snakemake_metadata is not None
        name = metadata._snakemake_metadata.name
        self.jit_wf_version = jit_wf_version
        self.jit_exec_display_name = jit_exec_display_name

        assert name is not None

        python_interface, literal_map, return_files = snakemake_dag_to_interface(
            dag,
            name,
            None,
            local_to_remote_path_mapping,
        )

        self.literal_map = literal_map
        self.return_files = return_files
        self._input_parameters = None
        self._dag = dag
        self.snakemake_tasks: List[SnakemakeJobTask] = []

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

    def compile(self, **kwargs):
        self._input_parameters = interface_to_parameters(self.python_interface)

        GLOBAL_START_NODE = Node(
            id=_common_constants.GLOBAL_INPUT_NODE_ID,
            metadata=None,
            bindings=[],
            upstream_nodes=[],
            flyte_entity=None,
        )

        node_map: Dict[str, Node] = {}

        target_files = [x for job in self._dag.targetjobs for x in job.input]

        for layer in self._dag.toposorted():
            for job in layer:
                assert isinstance(job, snakemake.jobs.Job)
                is_target = False

                if job in self._dag.targetjobs:
                    continue

                target_file_for_output_param: Dict[str, str] = {}
                target_file_for_input_param: Dict[str, str] = {}

                python_outputs: Dict[str, Union[Type[LatchFile], Type[LatchDir]]] = {}
                for x in job.output:
                    assert isinstance(x, SnakemakeInputVal)

                    if x in target_files:
                        is_target = True
                    param = variable_name_for_value(x, job.output)
                    target_file_for_output_param[param] = x

                    if x.is_directory:
                        python_outputs[param] = LatchDir
                    else:
                        python_outputs[param] = LatchFile

                for x in job.log:
                    assert isinstance(x, SnakemakeInputVal)

                    if x in target_files:
                        is_target = True
                    param = variable_name_for_value(x, job.log)
                    target_file_for_output_param[param] = x

                    if x.is_directory:
                        python_outputs[param] = LatchDir
                    else:
                        python_outputs[param] = LatchFile

                dep_outputs: Dict[SnakemakeInputVal, JobOutputInfo] = {}
                for dep, dep_files in self._dag.dependencies[job].items():
                    for o in dep.output:
                        if o in dep_files:
                            assert isinstance(o, SnakemakeInputVal)

                            dep_outputs[o] = JobOutputInfo(
                                jobid=dep.jobid,
                                output_param_name=variable_name_for_value(
                                    o, dep.output
                                ),
                                type_=LatchDir if o.is_directory else LatchFile,
                            )

                    for o in dep.log:
                        if o in dep_files:
                            assert isinstance(o, SnakemakeInputVal)

                            dep_outputs[o] = JobOutputInfo(
                                jobid=dep.jobid,
                                output_param_name=variable_name_for_value(o, dep.log),
                                type_=LatchDir if o.is_directory else LatchFile,
                            )

                python_inputs: Dict[str, Union[Type[LatchFile], Type[LatchDir]]] = {}
                promise_map: Dict[str, JobOutputInfo] = {}
                for x in job.input:
                    param = variable_name_for_value(x, job.input)
                    target_file_for_input_param[param] = x

                    dep_out = dep_outputs.get(x)

                    python_inputs[param] = LatchFile

                    if dep_out is not None:
                        python_inputs[param] = dep_out.type_
                        promise_map[param] = dep_out

                interface = Interface(python_inputs, python_outputs, docstring=None)
                task = SnakemakeJobTask(
                    wf=self,
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
                assert typed_interface is not None

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
                    id=f"n{job.jobid}",
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

        raise RuntimeError(f"could not find upstream node for output: {out_var}")

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)


def build_jit_register_wrapper() -> JITRegisterWorkflow:
    wrapper_wf = JITRegisterWorkflow()
    out_parameter_name = wrapper_wf.out_parameter_name

    python_interface = wrapper_wf.python_interface
    wrapper_wf._input_parameters = interface_to_parameters(python_interface)

    GLOBAL_START_NODE = Node(
        id=_common_constants.GLOBAL_INPUT_NODE_ID,
        metadata=None,
        bindings=[],
        upstream_nodes=[],
        flyte_entity=None,
    )
    task_interface = Interface(
        python_interface.inputs, python_interface.outputs, docstring=None
    )
    task = PythonAutoContainerTask[T](
        name=f"{wrapper_wf.name}_task",
        task_type="python-task",
        interface=task_interface,
        task_config=None,
        task_resolver=JITRegisterWorkflowResolver(),
    )

    typed_interface = transform_interface_to_typed_interface(python_interface)
    assert typed_interface is not None

    task_bindings: List[literals_models.Binding] = []
    for k in python_interface.inputs:
        var = typed_interface.inputs[k]
        promise_to_bind = Promise(
            var=k,
            val=NodeOutput(node=GLOBAL_START_NODE, var=k),
        )
        task_bindings.append(
            binding_from_python(
                var_name=k,
                expected_literal_type=var.type,
                t_value=promise_to_bind,
                t_value_type=python_interface.inputs[k],
            )
        )
    task_node = Node(
        id="n0",
        metadata=task.construct_node_metadata(),
        bindings=sorted(task_bindings, key=lambda b: b.var),
        upstream_nodes=[],
        flyte_entity=task,
    )

    promise_to_bind = Promise(
        var=out_parameter_name,
        val=NodeOutput(node=task_node, var=out_parameter_name),
    )
    t = python_interface.outputs[out_parameter_name]
    output_binding = binding_from_python(
        out_parameter_name,
        bool,
        promise_to_bind,
        t,
    )

    wrapper_wf._nodes = [task_node]
    wrapper_wf._output_bindings = [output_binding]
    return wrapper_wf


class AnnotatedStrJson(TypedDict):
    value: str
    flags: Dict[str, bool]


MaybeAnnotatedStrJson: TypeAlias = Union[str, AnnotatedStrJson]


def annotated_str_to_json(
    x: Union[str, snakemake.io._IOFile, snakemake.io.AnnotatedString]
) -> MaybeAnnotatedStrJson:
    if not isinstance(x, (snakemake.io.AnnotatedString, snakemake.io._IOFile)):
        return x

    flags = dict(x.flags.items())
    if "report" in flags:
        report = flags["report"]
        flags["report"] = {
            "caption": report.caption.get_filename(),
            "category": report.category,
        }

    return {"value": str(x), "flags": flags}


IONamedListItem = Union[MaybeAnnotatedStrJson, List[MaybeAnnotatedStrJson]]


class NamedListJson(TypedDict):
    positional: List[IONamedListItem]
    keyword: Dict[str, IONamedListItem]


def named_list_to_json(xs: snakemake.io.Namedlist) -> NamedListJson:
    named: Dict[str, IONamedListItem] = {}
    for k, vs in xs.items():
        if not isinstance(vs, list):
            named[k] = annotated_str_to_json(vs)
            continue

        named[k] = [annotated_str_to_json(v) for v in vs]

    named_values = set()
    for vs in named.values():
        if not isinstance(vs, list):
            vs = [vs]

        for v in vs:
            if isinstance(v, dict):
                v = v["value"]
            named_values.add(v)

    unnamed: List[IONamedListItem] = []
    for vs in xs:
        if not isinstance(vs, list):
            vs = [vs]

        for v in vs:
            obj = annotated_str_to_json(v)

            rendered = obj
            if isinstance(rendered, dict):
                rendered = rendered["value"]
            if rendered in named_values:
                continue

            unnamed.append(obj)

    return {"positional": unnamed, "keyword": named}


class SnakemakeJobTask(PythonAutoContainerTask[Pod]):
    def __init__(
        self,
        wf: SnakemakeWorkflow,
        job: snakemake.jobs.Job,
        inputs: Dict[str, Union[Type[LatchFile], Type[LatchDir]]],
        outputs: Dict[str, Union[Type[LatchFile], Type[LatchDir]]],
        target_file_for_input_param: Dict[str, str],
        target_file_for_output_param: Dict[str, str],
        is_target: bool,
        interface: Interface,
    ):
        name = f"{job.name}_{job.jobid}"

        self.wf = wf
        self.job = job
        self._is_target = is_target
        self._python_inputs = inputs
        self._python_outputs = outputs
        self._target_file_for_input_param = target_file_for_input_param
        self._target_file_for_output_param = target_file_for_output_param

        self._task_function = task_fn_placeholder

        limits = self.job.resources
        cores = limits.get("cpus", 4)

        # convert MB to GiB
        mem = limits.get("mem_mb", 8589) * 1000 * 1000 // 1024 // 1024 // 1024

        super().__init__(
            task_type="sidecar",
            task_type_version=2,
            name=name,
            interface=interface,
            task_config=custom_task(cpu=cores, memory=mem).keywords["task_config"],
            task_resolver=SnakemakeJobTaskResolver(),
        )

    # todo(maximsmol): this is very awful
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
        sdk_default_container = super().get_container(settings)
        for container in containers:
            # In the case of the primary container, we overwrite specific container attributes with the default values
            # used in the regular Python task.
            if container.name == self.task_config.primary_container_name:
                container.image = sdk_default_container.image
                # Spawn entrypoint as child process so it can receive signals
                container.command = [
                    "/bin/bash",
                    "-c",
                    (
                        "exec 3>&1 4>&2 && latch cp"
                        f' "latch:///.snakemake_latch/workflows/{self.wf.name}/{self.wf.jit_wf_version}/{self.wf.jit_exec_display_name}/entrypoint.py"'
                        " latch_entrypoint.py && ("
                        f" {' '.join(sdk_default_container.args)} 1>&3 2>&4 )"
                    ),
                ]
                container.args = []

                limits, requests = {}, {}
                for resource in sdk_default_container.resources.limits:
                    limits[_sanitize_resource_name(resource)] = resource.value
                for resource in sdk_default_container.resources.requests:
                    requests[_sanitize_resource_name(resource)] = resource.value

                resource_requirements = V1ResourceRequirements(
                    limits=limits, requests=requests
                )
                if len(limits) > 0 or len(requests) > 0:
                    # Important! Only copy over resource requirements if they are non-empty.
                    container.resources = resource_requirements

                container.env = [
                    V1EnvVar(name=key, value=val)
                    for key, val in sdk_default_container.env.items()
                ]

            final_containers.append(container)

        self.task_config._pod_spec.containers = final_containers

        return ApiClient().sanitize_for_serialization(self.task_config.pod_spec)

    def get_k8s_pod(self, settings: SerializationSettings) -> _task_models.K8sPod:
        return _task_models.K8sPod(
            pod_spec=self._serialize_pod_spec(settings),
            metadata=_task_models.K8sObjectMetadata(
                labels=self.task_config.labels,
                annotations=self.task_config.annotations,
            ),
        )

    def get_container(self, settings: SerializationSettings) -> _task_models.Container:
        return None

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        return {_PRIMARY_CONTAINER_NAME_FIELD: self.task_config.primary_container_name}

    def get_fn_interface(self):
        res = ""

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: {t.__name__}
                """,
                1,
            ).rstrip()
            for param, t in self._python_inputs.items()
        )

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

    def get_fn_return_stmt(self, remote_output_url: Optional[str] = None):
        print_outs: List[str] = []
        results: List[str] = []
        for out_name, out_type in self._python_outputs.items():
            target_path = self._target_file_for_output_param[out_name]

            print_outs.append(
                reindent(
                    rf"""
                    print(f'  {out_name}={{file_name_and_size(Path("{target_path}"))}}')
                    """,
                    1,
                )
            )

            if not self._is_target:
                results.append(
                    reindent(
                        rf"""
                        {out_name}={out_type.__name__}("{target_path}")
                        """,
                        2,
                    ).rstrip()
                )
                continue

            if remote_output_url is None:
                remote_path = Path("/Snakemake Outputs") / self.wf.name / target_path
            else:
                remote_path = Path(urlparse(remote_output_url).path) / target_path

            results.append(
                reindent(
                    rf"""
                    {out_name}={out_type.__name__}("{target_path}", "latch://{remote_path}")
                    """,
                    2,
                ).rstrip()
            )

        print_out_str = "\n".join(print_outs)
        return_str = ",\n".join(results)

        return (
            reindent(
                rf"""
                    print("Uploading results:")
                __print_out__

                    return Res{self.name}(
                __return_str__
                    )
            """,
                0,
            )
            .replace("__print_out__", print_out_str)
            .replace("__return_str__", return_str)
        )

    def get_fn_code(
        self,
        snakefile_path_in_container: str,
        remote_output_url: Optional[str] = None,
        non_blob_parameters: Optional[Dict[str, str]] = None,
    ):
        code_block = self.get_fn_interface()

        for param, t in self._python_inputs.items():
            if not issubclass(t, (LatchFile, LatchDir)):
                continue

            code_block += reindent(
                rf"""
                {param}_dst_p = Path("{self._target_file_for_input_param[param]}")

                print(f"Downloading {param}: {{{param}.remote_path}}")
                {param}_p = Path({param}).resolve()
                print(f"  {{file_name_and_size({param}_p)}}")

                """,
                1,
            )

            code_block += reindent(
                rf"""
                print(f"Moving {param} to {{{param}_dst_p}}")
                check_exists_and_rename(
                    {param}_p,
                    {param}_dst_p
                )
                """,
                1,
            )

        jobs: List[Job] = [self.job]
        if isinstance(self.job, GroupJob):
            jobs = self.job.jobs

        need_conda = any(x.conda_env is not None for x in jobs)

        if non_blob_parameters is not None and len(non_blob_parameters) > 0:
            self.job.rule.workflow.globals["config"] = non_blob_parameters

        snakemake_args = [
            "-m",
            "latch_cli.snakemake.single_task_snakemake",
            "-s",
            snakefile_path_in_container,
            *(["--use-conda"] if need_conda else []),
            "--target-jobs",
            *jobs_cli_args(jobs),
            "--allowed-rules",
            *self.job.rules,
            "--local-groupid",
            str(self.job.jobid),
            "--cores",
            str(self.job.threads),
        ]
        if not self.job.is_group():
            snakemake_args.append("--force-use-threads")

        excluded = {"_nodes", "_cores", "tmpdir"}
        allowed_resources = list(
            filter(lambda x: x[0] not in excluded, self.job.resources.items())
        )
        if len(allowed_resources) > 0:
            snakemake_args.append("--resources")
            for resource, value in allowed_resources:
                snakemake_args.append(f"{resource}={value}")

        snakemake_data = {
            "rules": {},
            "outputs": self.job.output,
            "non_blob_parameters": non_blob_parameters,
        }

        for job in jobs:
            snakemake_data["rules"][job.rule.name] = {
                "inputs": named_list_to_json(job.input),
                "outputs": named_list_to_json(job.output),
                "params": {
                    "keyword": {k: v for k, v in job.params.items()},
                    "positional": [],
                },
                "benchmark": job.benchmark,
                "log": job.log,
                "shellcmd": job.shellcmd,
            }

        if remote_output_url is None:
            remote_path = Path("/Snakemake Outputs") / self.wf.name
        else:
            remote_path = Path(urlparse(remote_output_url).path)

        log_files = self.job.log if self.job.log is not None else []

        code_block += reindent(
            rf"""
            lp = LatchPersistence()
            compiled = Path("compiled.py")
            print("Saving compiled Snakemake script")
            with compiled.open("w") as f:
                try:
                    subprocess.run(
                        [sys.executable,{','.join(repr(x) for x in [*snakemake_args, "--print-compilation"])}],
                        check=True,
                        env={{
                            **os.environ,
                            "LATCH_SNAKEMAKE_DATA": {repr(json.dumps(snakemake_data))},
                            "LATCH_PRINT_COMPILATION": "1"
                        }},
                        stdout=f
                    )
                except CalledProcessError:
                    print("  Failed")
                except Exception:
                    traceback.print_exc()
            lp.upload(compiled, "latch:///.snakemake_latch/workflows/{self.wf.name}/compiled_tasks/{self.name}.py")

            print("\n\n\nRunning snakemake task\n")
            try:
                log_files = {repr(log_files)}
                try:
                    tail = None
                    if len(log_files) == 1:
                        log = Path(log_files[0])
                        log.parent.mkdir(parents=True, exist_ok=True)
                        log.touch()

                        print(f"Tailing the only log file: {{log}}")
                        tail = subprocess.Popen(["tail", "--follow", log])

                    print("\n\n\n")
                    try:
                        subprocess.run(
                            [sys.executable,{','.join(repr(x) for x in snakemake_args)}],
                            check=True,
                            env={{
                                **os.environ,
                                "LATCH_SNAKEMAKE_DATA": {repr(json.dumps(snakemake_data))}
                            }}
                        )
                    finally:
                        if tail is not None:
                            import signal
                            tail.send_signal(signal.SIGINT)
                            try:
                                tail.wait(1)
                            except subprocess.TimeoutExpired:
                                tail.kill()

                            tail.wait()
                            # -2 is SIGINT
                            if tail.returncode != -2 and tail.returncode != 0:
                                print(f"\n\n\n[!] Log file tail died with code {{tail.returncode}}")

                    print("\n\n\nDone\n\n\n")
                except Exception as e:
                    print("\n\n\n[!] Failed\n\n\n")
                    raise e
                finally:
                    print("Uploading logs:")
                    for x in log_files:
                        local = Path(x)
                        remote = f"latch://{remote_path}/{{str(local).removeprefix('/')}}"
                        print(f"  {{file_name_and_size(local)}} -> {{remote}}")
                        if not local.exists():
                            print("  Does not exist")
                            continue

                        lp.upload(local, remote)
                        print("    Done")

                    benchmark_file = {repr(self.job.benchmark)}
                    if benchmark_file is not None:
                        print("\nUploading benchmark:")

                        local = Path(benchmark_file)
                        if local.exists():
                            print(local.read_text())

                            remote = f"latch://{remote_path}/{{str(local).removeprefix('/')}}"
                            print(f"  {{file_name_and_size(local)}} -> {{remote}}")
                            lp.upload(local, remote)
                            print("    Done")
                        else:
                            print("  Does not exist")

            finally:
                ignored_paths = {{".cache", ".snakemake/conda"}}
                ignored_names = {{".git", ".latch", "__pycache__"}}
            """,
            1,
        )

        code_block += self.get_fn_return_stmt(remote_output_url=remote_output_url)
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


class JITRegisterWorkflowResolver(DefaultTaskResolver):
    @property
    def location(self) -> str:
        return "flytekit.core.python_auto_container.default_task_resolver"

    def loader_args(
        self, settings: SerializationSettings, task: PythonAutoContainerTask[T]
    ) -> List[str]:
        return ["task-module", "snakemake_jit_entrypoint", "task-name", task.name]

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        _, task_module, _, task_name, *_ = loader_args

        task_module = importlib.import_module(task_module)

        task_def = getattr(task_module, task_name)
        return task_def
