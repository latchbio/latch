import importlib
import textwrap
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union
from urllib.parse import urlparse

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
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralMap, Scalar
from snakemake.dag import DAG
from snakemake.target_jobs import encode_target_jobs_cli_args
from typing_extensions import TypeAlias

import latch.types.metadata as metadata
from latch.types.directory import LatchDir
from latch.types.file import LatchFile

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


@dataclass
class RemoteFile:
    local_path: str
    remote_path: str


def snakemake_dag_to_interface(
    dag: DAG, wf_name: str, docstring: Optional[Docstring] = None
) -> (Interface, LiteralMap, List[RemoteFile]):
    outputs: Dict[str, LatchFile] = {}
    for target in dag.targetjobs:
        for x in target.input:
            param = variable_name_for_value(x, target.input)
            outputs[param] = LatchFile

    literals: Dict[str, Literal] = {}
    inputs: Dict[str, Tuple[LatchFile, None]] = {}
    return_files: List[RemoteFile] = []
    for job in dag.jobs:
        dep_outputs = []
        for dep, dep_files in dag.dependencies[job].items():
            for o in dep.output:
                if o in dep_files:
                    dep_outputs.append(o)

        for x in job.input:
            if x not in dep_outputs:
                param = variable_name_for_value(x, job.input)
                inputs[param] = (
                    LatchFile,
                    None,
                )
                remote_path = Path("/.snakemake_latch") / wf_name / x
                remote_url = f"latch://{remote_path}"
                return_files.append(RemoteFile(local_path=x, remote_path=remote_url))
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
                        )
                    )
                )

    return (
        Interface(inputs, outputs, docstring=docstring),
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


class JITRegisterWorkflow(WorkflowBase, ClassStorageTaskResolver):
    out_parameter_name = "success"

    def __init__(self):
        meta = metadata._snakemake_metadata
        parameter_metadata = meta.parameters
        display_name = meta.display_name
        name = meta.name

        docstring = Docstring(f"{display_name}\n\nSample Description\n\n" + str(meta))
        native_interface = Interface(
            {k: v.type for k, v in parameter_metadata.items()},
            {self.out_parameter_name: bool},
            docstring=docstring,
        )

        self.parameter_metadata = parameter_metadata
        if meta.output_dir is not None:
            self.remote_output_url = meta.output_dir.remote_path
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
            python_interface=native_interface,
        )

    def get_fn_interface(
        self, decorator_name="small_task", fn_name: Optional[str] = None
    ):
        if fn_name is None:
            fn_name = self.name
        fn_interface = f"\n\n@{decorator_name}\ndef {fn_name}("
        fn_interface += (
            ", ".join(
                f"{param}: {repr(__name__)}"
                for param, t in self.python_interface.inputs.items()
            )
            + ") -> bool:"
        )
        return fn_interface

    def get_fn_return_stmt(self):
        return "\n\treturn True"

    def get_fn_code(
        self,
        snakefile_path: str,
        version: str,
        image_name: str,
        account_id: str,
        remote_output_url: Optional[str],
    ):
        task_name = f"{self.name}_task"
        code_block = ""
        code_block += self.get_fn_interface(fn_name=task_name)

        for param, t in self.python_interface.inputs.items():
            if t in (LatchFile, LatchDir):
                code_block += (
                    f"\n\tcheck_exists_and_rename(Path({param}).resolve(),"
                    f' Path("{self.parameter_metadata[param].path}"))'
                )
            else:
                raise ValueError(f"Unsupported parameter type {t} for {param}")

        code_block += textwrap.indent(
            textwrap.dedent(f"""

        image_name = "{image_name}"
        account_id = "{account_id}"
        snakefile = Path("{snakefile_path}")

        """),
            "\t",
        )

        if remote_output_url is not None:
            remote_output_url = f"'{remote_output_url}'"
        code_block += textwrap.indent(
            textwrap.dedent(f"""
        pkg_root = Path(".")
        version = hashlib.sha1(os.environ["FLYTE_INTERNAL_EXECUTION_ID"].encode("utf-8")).hexdigest()

        wf = extract_snakemake_workflow(pkg_root, snakefile, version)
        wf_name = wf.name
        generate_snakemake_entrypoint(wf, pkg_root, snakefile, {remote_output_url})
        """),
            "\t",
        )

        code_block += textwrap.indent(
            textwrap.dedent("""
        dockerfile = Path("Dockerfile-dynamic").resolve()
        dockerfile.write_text(
            textwrap.dedent(
                f'''
            from 812206152185.dkr.ecr.us-west-2.amazonaws.com/{image_name}

            copy latch_entrypoint.py /root/latch_entrypoint.py
            '''
            )
        )
        new_image_name = f"{image_name}-{version}"

        os.mkdir("/root/.ssh")
        ssh_key_path = Path("/root/.ssh/id_rsa")
        cmd = ["ssh-keygen", "-f", ssh_key_path, "-N", "", "-q"]
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            raise ValueError(
                "There was a problem creating temporary SSH credentials. Please ensure"
                " that `ssh-keygen` is installed and available in your PATH."
            ) from e
        os.chmod(ssh_key_path, 0o700)

        token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID", "")
        headers = {
            "Authorization": f"Latch-Execution-Token {token}",
        }

        ssh_public_key_path = Path("/root/.ssh/id_rsa.pub")
        response = tinyrequests.post(
            config.api.centromere.provision,
            headers=headers,
            json={
                "public_key": ssh_public_key_path.read_text().strip(),
            },
        )

        resp = response.json()
        try:
            public_ip = resp["ip"]
            username = resp["username"]
        except KeyError as e:
            raise ValueError(
                f"Malformed response from request for centromere login: {resp}"
            ) from e


        subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", f"{username}@{public_ip}", "uptime"])
        dkr_client = _construct_dkr_client(ssh_host=f"ssh://{username}@{public_ip}")

        data = {"pkg_name": new_image_name.split(":")[0], "ws_account_id": account_id}
        response = requests.post(config.api.workflow.upload_image, headers=headers, json=data)

        try:
            response = response.json()
            access_key = response["tmp_access_key"]
            secret_key = response["tmp_secret_key"]
            session_token = response["tmp_session_token"]
        except KeyError as err:
            raise ValueError(f"malformed response on image upload: {response}") from err

        try:
            client = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name="us-west-2",
            ).client("ecr")
            token = client.get_authorization_token()["authorizationData"][0][
                "authorizationToken"
            ]
        except Exception as err:
            raise ValueError(
                f"unable to retreive an ecr login token for user {account_id}"
            ) from err

        user, password = base64.b64decode(token).decode("utf-8").split(":")
        dkr_client.login(
            username=user,
            password=password,
            registry=config.dkr_repo,
        )

        image_build_logs = dkr_client.build(
            path=str(pkg_root),
            dockerfile=str(dockerfile),
            buildargs={"tag": f"{config.dkr_repo}/{new_image_name}"},
            tag=f"{config.dkr_repo}/{new_image_name}",
            decode=True,
        )
        print_and_write_build_logs(image_build_logs, new_image_name, pkg_root)

        upload_image_logs = dkr_client.push(
            repository=f"{config.dkr_repo}/{new_image_name}",
            stream=True,
            decode=True,
        )
        print_upload_logs(upload_image_logs, new_image_name)

        temp_dir = tempfile.TemporaryDirectory()
        with Path(temp_dir.name).resolve() as td:
            serialize_snakemake(wf, td, new_image_name, config.dkr_repo)

            protos = _recursive_list(td)
            reg_resp = register_serialized_pkg(protos, None, version, account_id)
            _print_reg_resp(reg_resp, new_image_name)


        class _WorkflowInfoNode(TypedDict):
            id: str


        nodes: Optional[List[_WorkflowInfoNode]] = None
        while not nodes:
            time.sleep(1)
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

        if len(nodes) > 1:
            raise ValueError(
                "Invariant violated - more than one workflow identified for unique combination"
                " of {wf_name}, {version}, {account_id}"
            )

        print(nodes)

        lp = LatchPersistence()
        for file in wf.return_files:
            print(f"Uploading {file.local_path} -> {file.remote_path}")
            lp.upload(file.local_path, file.remote_path)

        wf_id = nodes[0]["id"]
        params = json.loads(gpjson.MessageToJson(wf.literal_map.to_flyte_idl()))["literals"]

        _interface_request = {
            "workflow_id": wf_id,
            "params": params,
        }

        response = requests.post(config.api.workflow.create_execution, headers=headers, json=_interface_request)
        print(response.json())
        """),
            "\t",
        )
        code_block += self.get_fn_return_stmt()
        return code_block


def build_jit_register_wrapper() -> JITRegisterWorkflow:
    out_parameter_name = "success"
    wrapper_wf = JITRegisterWorkflow()

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

    task_bindings: List[literals_models.Binding] = []
    typed_interface = transform_interface_to_typed_interface(python_interface)
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


class SnakemakeWorkflow(WorkflowBase, ClassStorageTaskResolver):
    def __init__(
        self,
        dag: DAG,
        version: Optional[str] = None,
    ):
        meta = metadata._snakemake_metadata
        if version is not None:
            name = f"{meta.name}-{version}"
        else:
            name = meta.name

        assert (
            name is not None
        )  # todo(maximsmol): this should really be fixed in the dataclass

        native_interface, literal_map, return_files = snakemake_dag_to_interface(
            dag, name, None
        )
        self.literal_map = literal_map
        self.return_files = return_files
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

    def get_fn_interface(self):
        fn_interface = f"\n\n@small_task\ndef {self.name}("
        fn_interface += (
            ", ".join(f"{param}: {repr(t)}" for param, t in self._python_inputs.items())
            + ")"
        )

        if len(self._python_outputs.items()) > 0:
            fn_interface += (
                f" -> NamedTuple('{self.name}_output', "
                + ", ".join(
                    f"{param}={repr(t)}" for param, t in self._python_outputs.items()
                )
                + "):"
            )
        else:
            fn_interface += ":"

        return fn_interface

    def get_fn_return_stmt(self, remote_output_url: Optional[str] = None):
        return_stmt = "\n\treturn ("
        for i, x in enumerate(self._python_outputs):
            if self._is_target:
                target_path = self._target_file_for_output_param[x]
                if remote_output_url is None:
                    remote_path = Path("/Snakemake Outputs") / target_path
                else:
                    remote_path = Path(urlparse(remote_output_url).path) / target_path

                return_stmt += f"LatchFile('{target_path}', 'latch://{remote_path}')"
            else:
                return_stmt += f"LatchFile('{self._target_file_for_output_param[x]}')"
            if i == len(self._python_outputs) - 1:
                return_stmt += ")"
            else:
                return_stmt += ", "

        return return_stmt

    def get_fn_code(
        self, snakefile_path_in_container: str, remote_output_url: Optional[str] = None
    ):
        code_block = ""
        code_block += self.get_fn_interface()

        for param, t in self._python_inputs.items():
            if t == LatchFile:
                code_block += (
                    f"\n\tcheck_exists_and_rename(Path({param}).resolve(),"
                    f' Path("{self._target_file_for_input_param[param]}"))'
                )

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
        return ["task-module", "jit_entrypoint", "task-name", task.name]

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        _, task_module, _, task_name, *_ = loader_args

        task_module = importlib.import_module(task_module)

        task_def = getattr(task_module, task_name)
        return task_def
