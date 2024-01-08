import importlib
import textwrap
from pathlib import Path
from typing import List, Optional, TypeVar

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
from flytekit.core.workflow import (
    WorkflowBase,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.models import literals as literals_models

from latch.types import metadata
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.extras.snakemake.config.utils import type_repr
from latch_cli.extras.snakemake.workflow import (
    binding_from_python,
    interface_to_parameters,
)

T = TypeVar("T")


def reindent(x: str, level: int) -> str:
    if x[0] == "\n":
        x = x[1:]
    return textwrap.indent(textwrap.dedent(x), "    " * level)


class NFJITRegisterWorkflowResolver(DefaultTaskResolver):
    @property
    def location(self) -> str:
        return "flytekit.core.python_auto_container.default_task_resolver"

    def loader_args(
        self, settings: SerializationSettings, task: PythonAutoContainerTask[T]
    ) -> List[str]:
        return ["task-module", "nextflow_jit_entrypoint", "task-name", task.name]

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        _, task_module, _, task_name, *_ = loader_args

        task_module = importlib.import_module(task_module)

        task_def = getattr(task_module, task_name)
        return task_def


class NFJITRegisterWorkflow(WorkflowBase, ClassStorageTaskResolver):
    out_parameter_name = "o0"  # must be "o0"

    def __init__(
        self,
    ):
        parameter_metadata = metadata._nextflow_metadata.parameters
        display_name = metadata._nextflow_metadata.display_name
        name = metadata._nextflow_metadata.name

        docstring = Docstring(
            f"{display_name}\n\nSample Description\n\n"
            + str(metadata._nextflow_metadata)
        )
        python_interface = Interface(
            {k: (v.type, v.default) for k, v in parameter_metadata.items()},
            {self.out_parameter_name: bool},
            docstring=docstring,
        )
        self.parameter_metadata = parameter_metadata

        # TODO
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
            """
            return True
            """,
            1,
        )

    def get_fn_code(
        self,
        version: str,
        image_name: str,
        account_id: str,
    ):
        task_name = f"{self.name}_task"

        code_block = self.get_fn_interface(fn_name=task_name)

        code_block += reindent(
            r"""
            local_to_remote_path_mapping = {}
            """,
            1,
        )

        for param, t in self.python_interface.inputs.items():
            param_meta = self.parameter_metadata[param]

            if t in (LatchFile, LatchDir):
                # TODO
                # assert isinstance(param_meta, metadata.SnakemakeFileParameter)

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

                    update_mapping({param}_p, {param}.remote_path, local_to_remote_path_mapping)
                    check_exists_and_rename(
                        {param}_p,
                        {param}_dst_p
                    )

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
                """,
                1,
            )

        code_block += reindent(
            rf"""
            image_name = "{image_name}"

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
            res = execute(
                gql.gql('''
                query executionCreatorsByToken($token: String!) {
                    executionCreatorByToken(token: $token) {
                        flytedbId
                        info {
                            displayName
                        }
                        accountInfoByCreatedBy {
                            id
                        }
                    }
                }
                '''),
                {"token": token},
            )["executionCreatorByToken"]

            jit_exec_display_name = res["info"]["displayName"]
            account_id = res["accountInfoByCreatedBy"]["id"]
            """,
            1,
        )

        code_block += reindent(
            rf"""
            print(f"JIT Workflow Version: {{jit_wf_version}}")
            print(f"JIT Execution Display Name: {{jit_exec_display_name}}")

            wf = build_nf_wf()
            wf_name = wf.name
            # generate_nf_entrypoint(wf, pkg_root)
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
                serialize_nf(wf, td, image_name, config.dkr_repo)
                protos = _recursive_list(td)
                register_serialized_pkg(protos, None, version, account_id)

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


def build_nf_jit_register_wrapper() -> NFJITRegisterWorkflow:
    wrapper_wf = NFJITRegisterWorkflow()
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
        task_resolver=NFJITRegisterWorkflowResolver(),
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


def generate_nf_jit_register_code(
    wf: NFJITRegisterWorkflow,
    pkg_root: Path,
    version: str,
    image_name: str,
    account_id: str,
) -> Path:
    code_block = textwrap.dedent(r"""
        import json
        import os
        import subprocess
        import tempfile
        import textwrap
        import time
        import sys
        from functools import partial
        from pathlib import Path
        import shutil
        import typing
        from typing import NamedTuple, Optional, TypedDict, Dict, List
        import hashlib
        from urllib.parse import urljoin
        from dataclasses import is_dataclass, asdict
        from enum import Enum

        import stat
        import base64
        import boto3
        import boto3.session
        import google.protobuf.json_format as gpjson
        import gql
        import requests
        from flytekit.core import utils
        from flytekit.extras.persistence import LatchPersistence
        from latch_cli import tinyrequests
        from latch_cli.centromere.utils import _construct_dkr_client
        from latch_sdk_config.latch import config
        from latch_cli.services.register.register import (
            _print_reg_resp,
            _recursive_list,
            register_serialized_pkg,
            print_and_write_build_logs,
            print_upload_logs,
        )
        from latch_cli.utils import get_parameter_json_value, check_exists_and_rename
        from latch_cli.nextflow.workflow import build_nf_wf, generate_nf_entrypoint
        from latch_cli.nextflow.serialize import serialize_nf
        from latch_cli.utils import urljoins

        from latch import small_task
        from latch_sdk_gql.execute import execute
        from latch.types.directory import LatchDir
        from latch.types.file import LatchFile

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)

        def update_mapping(local: Path, remote: str, mapping: Dict[str, str]):
            if local.is_file():
                mapping[str(local)] = remote
                return

            for p in local.iterdir():
                update_mapping(p, urljoins(remote, p.name), mapping)

        def si_unit(num, base: float = 1000.0):
            for unit in (" ", "k", "M", "G", "T", "P", "E", "Z"):
                if abs(num) < base:
                    return f"{num:3.1f}{unit}"
                num /= base
            return f"{num:.1f}Y"

        def file_name_and_size(x: Path):
            s = x.stat()

            if stat.S_ISDIR(s.st_mode):
                return f"{'D':>8} {x.name}/"

            return f"{si_unit(s.st_size):>7}B {x.name}"

    """).lstrip()
    code_block += wf.get_fn_code(
        version,
        image_name,
        account_id,
    )

    entrypoint = pkg_root / ".latch" / "nextflow_jit_entrypoint.py"
    entrypoint.parent.mkdir(parents=True, exist_ok=True)
    entrypoint.write_text(code_block + "\n")

    return entrypoint
