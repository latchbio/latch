import os
import textwrap
from itertools import chain, filterfalse
from pathlib import Path
from typing import List, Optional, Set, Union, get_args

from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import literals as literals_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.tools.serialize_helpers import persist_registrable_entities
from snakemake.dag import DAG
from snakemake.persistence import Persistence
from snakemake.rules import Rule
from snakemake.workflow import Workflow

from latch.types.directory import LatchDir
from latch_cli.centromere.ctx import _CentromereCtx
from latch_cli.snakemake.serialize_utils import (
    EntityCache,
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from latch_cli.snakemake.workflow import (
    JITRegisterWorkflow,
    SnakemakeWorkflow,
    interface_to_parameters,
)

RegistrableEntity = Union[
    task_models.TaskSpec,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]


def should_register_with_admin(entity: RegistrableEntity) -> bool:
    return isinstance(entity, get_args(RegistrableEntity))


class SnakemakeWorkflowExtractor(Workflow):
    def __init__(self, snakefile: Path):
        super().__init__(snakefile=snakefile)

    def extract_dag(self):
        targets: List[str] = (
            [self.default_target] if self.default_target is not None else []
        )
        target_rules: Set[Rule] = set(
            map(self._rules.__getitem__, filter(self.is_rule, targets))
        )

        target_files = set()
        for f in filterfalse(self.is_rule, targets):
            if os.path.isabs(f) or f.startswith("root://"):
                target_files.add(f)
            else:
                target_files.add(os.path.relpath(f))

        dag = DAG(
            self,
            self.rules,
            targetfiles=target_files,
            targetrules=target_rules,
        )

        self.persistence = Persistence(
            dag=dag,
        )

        dag.init()
        dag.update_checkpoint_dependencies()
        dag.check_dynamic()

        return dag


def extract_snakemake_workflow(snakefile: Path) -> SnakemakeWorkflow:
    workflow = SnakemakeWorkflowExtractor(
        snakefile=snakefile,
    )
    workflow.include(
        snakefile,
        overwrite_default_target=True,
    )
    dag = workflow.extract_dag()
    wf = SnakemakeWorkflow(
        dag,
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

    get_serializable_workflow(wf, settings, registrable_entity_cache)

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
    persist_registrable_entities(registrable_entities, output_dir)


def serialize_jit_register_workflow(
    jit_wf: JITRegisterWorkflow,
    pkg_root: Path,
    snakefile: Path,
    output_dir: Path,
    image_name: str,
    dkr_repo: str,
):
    pkg_root = Path(pkg_root).resolve()
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
    persist_registrable_entities(registrable_entities, output_dir)


def snakefile_path_in_container(snakefile: Path, pkg_root: Path) -> str:
    return str(snakefile.resolve())[len(str(pkg_root.resolve())) + 1 :]


def generate_snakemake_entrypoint(
    wf: SnakemakeWorkflow,
    pkg_root: Path,
    snakefile: Path,
    remote_output_url: Optional[str] = None,
):
    entrypoint_code_block = textwrap.dedent("""\
           import os
           from pathlib import Path
           import shutil
           import subprocess
           from typing import NamedTuple

           from latch import small_task
           from latch.types.file import LatchFile

           def check_exists_and_rename(old: Path, new: Path):
               if new.exists():
                   print(f"A file already exists at {new} and will be overwritten.")
                   if new.is_dir():
                       shutil.rmtree(new)
               os.renames(old, new)
           """)
    for task in wf.snakemake_tasks:
        entrypoint_code_block += task.get_fn_code(
            snakefile_path_in_container(snakefile, pkg_root), remote_output_url
        )

    entrypoint = pkg_root.joinpath("latch_entrypoint.py")
    with open(entrypoint, "w") as f:
        f.write(entrypoint_code_block)


def generate_jit_register_code(
    wf: JITRegisterWorkflow,
    pkg_root: Path,
    snakefile: Path,
    version: str,
    image_name: str,
    account_id: str,
) -> Path:
    code_block = textwrap.dedent("""\
           import inspect
           import json
           import os
           import subprocess
           import tempfile
           import textwrap
           import time
           from functools import partial
           from pathlib import Path
           import shutil
           from typing import List, NamedTuple, Optional, TypedDict

           import base64
           import boto3
           import google.protobuf.json_format as gpjson
           import gql
           import requests
           from flyteidl.core import literals_pb2 as _literals_pb2
           from flytekit.core import utils
           from flytekit.core.context_manager import FlyteContext
           from flytekit.extras.persistence import LatchPersistence
           from latch_cli import tinyrequests
           from latch_cli.centromere.utils import _construct_dkr_client
           from latch_cli.config.latch import config
           from latch_cli.services.register.register import (_print_reg_resp,
                                                             _recursive_list,
                                                             register_serialized_pkg,
                                                             print_and_write_build_logs,
                                                             print_upload_logs)
           from latch_cli.services.serialize import (extract_snakemake_workflow,
                                                     generate_snakemake_entrypoint,
                                                     serialize_snakemake)
           from latch_cli.utils import generate_temporary_ssh_credentials

           from latch import small_task, workflow
           from latch.gql._execute import execute
           from latch.types.directory import LatchDir
           from latch.types.file import LatchFile


           print = partial(print, flush=True)

           def check_exists_and_rename(old: Path, new: Path):
               if new.exists():
                   print(f"A file already exists at {new} and will be overwritten.")
                   if new.is_dir():
                       shutil.rmtree(new)
               os.renames(old, new)
           """)
    code_block += wf.get_fn_code(
        snakefile_path_in_container(snakefile, pkg_root),
        version,
        image_name,
        account_id,
        wf.remote_output_url,
    )

    entrypoint = pkg_root.joinpath(".latch/jit_entrypoint.py")
    with open(entrypoint, "w") as f:
        f.write(code_block)
    return entrypoint
