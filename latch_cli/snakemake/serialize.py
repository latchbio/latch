import json
import os
import sys
import textwrap
import traceback
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Set, Union, get_args

import click
from flyteidl.admin.launch_plan_pb2 import LaunchPlan as _idl_admin_LaunchPlan
from flyteidl.admin.task_pb2 import TaskSpec as _idl_admin_TaskSpec
from flyteidl.admin.workflow_pb2 import WorkflowSpec as _idl_admin_WorkflowSpec
from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import literals as literals_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.tools.serialize_helpers import persist_registrable_entities
from google.protobuf.json_format import MessageToJson
from snakemake.dag import DAG
from snakemake.persistence import Persistence
from snakemake.rules import Rule
from snakemake.workflow import Workflow, WorkflowError
from typing_extensions import Self

import latch.types.metadata as metadata

from ..services.register.utils import import_module_by_path
from .serialize_utils import (
    EntityCache,
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from .utils import load_snakemake_metadata
from .workflow import JITRegisterWorkflow, SnakemakeWorkflow, interface_to_parameters

RegistrableEntity = Union[
    task_models.TaskSpec,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]


def should_register_with_admin(entity: RegistrableEntity) -> bool:
    return isinstance(entity, get_args(RegistrableEntity))


def get_snakemake_metadata_example(name: str) -> str:
    return dedent(f"""
        from pathlib import Path
        from latch.types.metadata import SnakemakeMetadata, SnakemakeFileParameter
        from latch.types.file import LatchFile
        from latch.types.metadata import LatchAuthor

        SnakemakeMetadata(
            display_name={repr(name)},
            author=LatchAuthor(
                name="Anonymous",
            ),
            parameters={{
                "example": SnakemakeFileParameter(
                    display_name="Example Parameter",
                    type=LatchFile,
                    path=Path("example.txt"),
                )
            }},
        )
        """).lstrip()


def ensure_snakemake_metadata_exists():
    if metadata._snakemake_metadata is None:
        click.secho(
            dedent("""
                    No `SnakemakeMetadata` object was detected in your project. This
                    object needs to be defined to register this workflow with Latch.

                    Create a file named `latch_metadata.py` with the following
                    code to get started:

                    __example__

                    Find more information at docs.latch.bio.
                    """).replace(
                "__example__", get_snakemake_metadata_example("example_name")
            ),
            bold=True,
            fg="red",
        )


# todo(maximsmol): this needs to run in a subprocess because it pollutes globals
class SnakemakeWorkflowExtractor(Workflow):
    def __init__(
        self,
        pkg_root: Path,
        snakefile: Path,
        overwrite_config: Optional[Dict[str, Any]] = None,
    ):
        assert metadata._snakemake_metadata is not None
        cores = metadata._snakemake_metadata.cores
        super().__init__(
            snakefile=snakefile, overwrite_config=overwrite_config, cores=cores
        )

        self.pkg_root = pkg_root
        self._old_cwd = ""

        if overwrite_config is not None:
            print(f"Config: {json.dumps(overwrite_config, indent=2)}")

    def extract_dag(self):
        targets: List[str] = (
            [self.default_target] if self.default_target is not None else []
        )
        target_rules: Set[Rule] = set(
            self._rules[x] for x in targets if self.is_rule(x)
        )

        target_files: Set[str] = set()
        for f in targets:
            if self.is_rule(f):
                continue

            if os.path.isabs(f) or f.startswith("root://"):
                target_files.add(f)
            else:
                target_files.add(os.path.relpath(f))

        dag = DAG(
            self,
            self.rules,
            targetfiles=target_files,
            targetrules=target_rules,
            priorityrules=set(),
            priorityfiles=set(),
        )

        try:
            self.persistence = Persistence(dag=dag)
        except AttributeError:
            self._persistence = Persistence(dag=dag)

        dag.init()
        dag.update_checkpoint_dependencies()
        dag.check_dynamic()

        return dag

    def __enter__(self) -> Self:
        self._old_cwd = os.getcwd()
        os.chdir(self.pkg_root)

        return self

    def __exit__(self, typ, value, tb):
        os.chdir(self._old_cwd)

        if typ is None:
            return False

        if not isinstance(value, WorkflowError):
            return False

        msg = str(value)
        if (
            "Workflow defines configfile config.yaml but it is not present or"
            " accessible"
            in msg
        ):
            # todo(maximsmol): print the expected config path
            traceback.print_exception(typ, value, tb)
            click.secho("\n\n\nHint: ", fg="red", bold=True, nl=False, err=True)
            click.secho("Snakemake could not find a config file", fg="red", err=True)
            sys.exit(1)

        # todo(maximsmol): handle specific errors
        # WorkflowError: Failed to open source file /Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk
        # FileNotFoundError: [Errno 2] No such file or directory: '/Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk'
        raise RuntimeError("invalid Snakefile") from value


def snakemake_workflow_extractor(
    pkg_root: Path,
    snakefile: Path,
    overwrite_config: Optional[Dict[str, Any]] = None,
) -> SnakemakeWorkflowExtractor:
    snakefile = snakefile.resolve()

    load_snakemake_metadata(pkg_root)

    extractor = SnakemakeWorkflowExtractor(
        pkg_root=pkg_root,
        snakefile=snakefile,
        overwrite_config=overwrite_config,
    )
    with extractor:
        extractor.include(
            snakefile,
            overwrite_default_target=True,
        )
        ensure_snakemake_metadata_exists()

    return extractor


def extract_snakemake_workflow(
    pkg_root: Path,
    snakefile: Path,
    jit_wf_version: str,
    jit_exec_display_name: str,
    local_to_remote_path_mapping: Optional[Dict[str, str]] = None,
    overwrite_config: Optional[Dict[str, Any]] = None,
    cache_tasks: bool = False,
) -> SnakemakeWorkflow:
    extractor = snakemake_workflow_extractor(pkg_root, snakefile, overwrite_config)
    with extractor:
        dag = extractor.extract_dag()
        wf = SnakemakeWorkflow(
            dag,
            jit_wf_version,
            jit_exec_display_name,
            local_to_remote_path_mapping,
            cache_tasks,
        )
        wf.compile()

    return wf


def serialize_snakemake(
    wf: SnakemakeWorkflow,
    output_dir: Path,
    image_name: str,
    dkr_repo: str,
):
    image_name_no_version, version = image_name.split(":")
    default_img = Image(
        name=image_name,
        fqn=f"{dkr_repo}/{image_name_no_version}",
        tag=version,
    )
    settings = SerializationSettings(
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    registrable_entity_cache: EntityCache = {}

    spec_dir = Path("spec")
    spec_dir.mkdir(parents=True, exist_ok=True)

    wf_spec = get_serializable_workflow(wf, settings, registrable_entity_cache)
    (spec_dir / "wf.json").write_text(MessageToJson(wf_spec.to_flyte_idl()))

    parameter_map = interface_to_parameters(wf.python_interface)
    lp = LaunchPlan(
        name=wf.name,
        workflow=wf,
        parameters=parameter_map,
        fixed_inputs=literals_models.LiteralMap(literals={}),
    )
    admin_lp = get_serializable_launch_plan(lp, settings, registrable_entity_cache)

    registrable_entities = [
        x.to_flyte_idl()
        for x in list(
            filter(should_register_with_admin, list(registrable_entity_cache.values()))
        )
        + [admin_lp]
    ]
    for idx, entity in enumerate(registrable_entities):
        cur = spec_dir

        if isinstance(entity, _idl_admin_TaskSpec):
            cur = cur / "tasks" / f"{entity.template.id.name}_{idx}.json"
        elif isinstance(entity, _idl_admin_WorkflowSpec):
            cur = cur / "wfs" / f"{entity.template.id.name}_{idx}.json"
        elif isinstance(entity, _idl_admin_LaunchPlan):
            cur = cur / "lps" / f"{entity.id.name}_{idx}.json"
        else:
            click.secho(
                f"Entity is incorrect formatted {entity} - type {type(entity)}",
                fg="red",
            )
            sys.exit(-1)

        cur.parent.mkdir(parents=True, exist_ok=True)
        cur.write_text(MessageToJson(entity))

    persist_registrable_entities(registrable_entities, str(output_dir))


def serialize_jit_register_workflow(
    jit_wf: JITRegisterWorkflow,
    output_dir: str,
    image_name: str,
    dkr_repo: str,
):
    image_name_no_version, version = image_name.split(":")
    default_img = Image(
        name=image_name,
        fqn=f"{dkr_repo}/{image_name_no_version}",
        tag=version,
    )
    settings = SerializationSettings(
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    registrable_entity_cache: EntityCache = {}

    get_serializable_workflow(jit_wf, settings, registrable_entity_cache)

    parameter_map = interface_to_parameters(jit_wf.python_interface)
    lp = LaunchPlan(
        name=jit_wf.name,
        workflow=jit_wf,
        parameters=parameter_map,
        fixed_inputs=literals_models.LiteralMap(literals={}),
    )
    admin_lp = get_serializable_launch_plan(lp, settings, registrable_entity_cache)

    registrable_entities = [
        x.to_flyte_idl()
        for x in list(
            filter(should_register_with_admin, list(registrable_entity_cache.values()))
        )
        + [admin_lp]
    ]

    click.secho("\nSerializing workflow entities", bold=True)
    persist_registrable_entities(registrable_entities, output_dir)


def snakefile_path_in_container(snakefile: Path, pkg_root: Path) -> str:
    return str(snakefile.resolve())[len(str(pkg_root.resolve())) + 1 :]


def generate_snakemake_entrypoint(
    wf: SnakemakeWorkflow,
    pkg_root: Path,
    snakefile: Path,
    remote_output_url: Optional[str] = None,
    overwrite_config: Optional[Dict[str, str]] = None,
):
    entrypoint_code_block = textwrap.dedent(r"""
        import os
        from pathlib import Path
        import shutil
        import subprocess
        from subprocess import CalledProcessError
        from typing import NamedTuple, Dict
        import stat
        import sys
        from dataclasses import is_dataclass, asdict
        from enum import Enum

        from flytekit.extras.persistence import LatchPersistence
        import traceback

        from latch.resources.tasks import custom_task
        from latch.types.directory import LatchDir
        from latch.types.file import LatchFile

        from latch_cli.utils import get_parameter_json_value, urljoins, check_exists_and_rename
        from latch_cli.snakemake.serialize_utils import update_mapping

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)


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

    entrypoint_code_block += "\n\n".join(
        task.get_fn_code(
            snakefile_path_in_container(snakefile, pkg_root),
            remote_output_url,
            overwrite_config,
        )
        for task in wf.snakemake_tasks
    )

    entrypoint = pkg_root / "latch_entrypoint.py"
    entrypoint.write_text(entrypoint_code_block + "\n")


def generate_jit_register_code(
    wf: JITRegisterWorkflow,
    pkg_root: Path,
    snakefile: Path,
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
        from typing import NamedTuple, Optional, TypedDict, Dict
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
        from latch_cli.snakemake.serialize import (
            extract_snakemake_workflow,
            generate_snakemake_entrypoint,
            serialize_snakemake,
        )
        from latch_cli.utils import get_parameter_json_value, check_exists_and_rename
        import latch_cli.snakemake
        from latch_cli.snakemake.serialize_utils import update_mapping
        from latch_cli.utils import urljoins

        from latch import small_task
        from latch_sdk_gql.execute import execute
        from latch.types.directory import LatchDir
        from latch.types.file import LatchFile

        try:
            import latch_metadata.parameters as latch_metadata
        except ImportError:
            import latch_metadata

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)


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
        snakefile_path_in_container(snakefile, pkg_root),
        image_name,
        wf.remote_output_url,
    )

    entrypoint = pkg_root / ".latch" / "snakemake_jit_entrypoint.py"
    entrypoint.parent.mkdir(parents=True, exist_ok=True)
    entrypoint.write_text(code_block + "\n")

    return entrypoint
