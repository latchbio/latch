import os
import textwrap
from itertools import filterfalse
from pathlib import Path
from textwrap import dedent
from typing import List, Optional, Set, TypeVar, Union, get_args

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
from snakemake.workflow import Workflow, WorkflowError

import latch.types.metadata as metadata
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

from ..services.register.utils import import_module_by_path

RegistrableEntity = Union[
    task_models.TaskSpec,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]


def should_register_with_admin(entity: RegistrableEntity) -> bool:
    return isinstance(entity, get_args(RegistrableEntity))


def ensure_snakemake_metadata_exists():
    if metadata._snakemake_metadata is None:
        raise ValueError(dedent("""

        No `SnakemakeMetadata` object was detected in your Snakefile. This
        object needs to be defined to register this workflow with Latch.

        You can paste the following in the top of your Snakefile to get
        started:

        ```
        from latch.types.metadata import SnakemakeMetadata, SnakemakeFileParameter
        from latch.types.file import LatchFile
        from latch.types.metadata import LatchAuthor, LatchMetadata

        SnakemakeMetadata(
            display_name="My Snakemake Workflow",
            author=LatchAuthor(
                    name="John Doe",
            ),
            parameters={
                "foo" : SnakemakeFileParameter(
                        display_name="Some Param",
                        type=LatchFile,
                        path=Path("foo.txt"),
                )
            }
        )
        ```

        Find more information at docs.latch.bio.
        """))


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


def snakemake_workflow_extractor(
    pkg_root: Path, snakefile: Path
) -> SnakemakeWorkflowExtractor:
    # todo(maximsmol): maybe switch to using a metadata file
    # metadata_module = import_module_by_path(pkg_root / "metadata.py")

    old_cwd = os.getcwd()
    snakefile = snakefile.resolve()
    try:
        os.chdir(pkg_root)

        workflow = SnakemakeWorkflowExtractor(
            snakefile=snakefile,
        )
        workflow.include(
            snakefile,
            overwrite_default_target=True,
        )

        ensure_snakemake_metadata_exists()

        return workflow
    except WorkflowError as e:
        # todo(maximsmol): handle specific errors
        # WorkflowError: Failed to open source file /Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk
        # FileNotFoundError: [Errno 2] No such file or directory: '/Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk'
        raise RuntimeError("invalid Snakefile") from e
    finally:
        os.chdir(old_cwd)


def extract_snakemake_workflow(
    pkg_root: Path, snakefile: Path, version: Optional[str] = None
) -> SnakemakeWorkflow:
    # todo(maximsmol): get rid of code duplication

    old_cwd = os.getcwd()
    snakefile = snakefile.resolve()
    try:
        os.chdir(pkg_root)

        workflow = snakemake_workflow_extractor(pkg_root, snakefile)

        dag = workflow.extract_dag()
        wf = SnakemakeWorkflow(dag, version)
        wf.compile()
        return wf
    except WorkflowError as e:
        # todo(maximsmol): handle specific errors
        # WorkflowError: Failed to open source file /Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk
        # FileNotFoundError: [Errno 2] No such file or directory: '/Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk'
        raise RuntimeError("invalid Snakefile") from e
    finally:
        os.chdir(old_cwd)


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
           import hashlib
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

    entrypoint = pkg_root / ".latch" / "jit_entrypoint.py"
    entrypoint.parent.mkdir(parents=True, exist_ok=True)

    entrypoint.write_text(code_block)

    return entrypoint
