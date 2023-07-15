import os
import textwrap
from itertools import filterfalse
from pathlib import Path
from textwrap import dedent
from typing import List, Optional, Set, Union, get_args

import click
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

from ..services.register.utils import import_module_by_path
from .serialize_utils import (
    EntityCache,
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from .workflow import JITRegisterWorkflow, SnakemakeWorkflow, interface_to_parameters

RegistrableEntity = Union[
    task_models.TaskSpec,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]


def should_register_with_admin(entity: RegistrableEntity) -> bool:
    return isinstance(entity, get_args(RegistrableEntity))


def ensure_snakemake_metadata_exists():
    import latch.types.metadata as metadata

    if metadata._snakemake_metadata is None:
        click.secho(
            dedent("""
                    No `SnakemakeMetadata` object was detected in your Snakefile. This
                    object needs to be defined to register this workflow with Latch.

                    You can paste the following in the top of your Snakefile to get
                    started:

                    ```
                    from pathlib import Path
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
                    """),
            bold=True,
            fg="red",
        )


class SnakemakeWorkflowExtractor(Workflow):
    def __init__(self, pkg_root: Path, snakefile: Path):
        super().__init__(snakefile=snakefile)

        self.pkg_root = pkg_root
        self._old_cwd = ""

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

    def __enter__(self) -> Self:
        self._old_cwd = os.getcwd()
        os.chdir(self.pkg_root)

        return self

    def __exit__(self, typ, value, traceback):
        os.chdir(self._old_cwd)

        if typ is None:
            return False

        if not isinstance(value, WorkflowError):
            return False

        # todo(maximsmol): handle specific errors
        # WorkflowError: Failed to open source file /Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk
        # FileNotFoundError: [Errno 2] No such file or directory: '/Users/maximsmol/projects/latchbio/latch/test/CGI_WGS_GATK_Pipeline/Snakefiles/CGI_WGS_GATK_Pipeline/Snakefiles/calc_frag_len.smk'
        raise RuntimeError("invalid Snakefile") from value


def snakemake_workflow_extractor(
    pkg_root: Path, snakefile: Path, version: Optional[str] = None
) -> SnakemakeWorkflowExtractor:
    snakefile = snakefile.resolve()

    meta = pkg_root / "latch_metadata.py"
    if meta.exists():
        import_module_by_path(meta)

    extractor = SnakemakeWorkflowExtractor(
        pkg_root=pkg_root,
        snakefile=snakefile,
    )
    with extractor:
        extractor.include(
            snakefile,
            overwrite_default_target=True,
        )
        ensure_snakemake_metadata_exists()

    return extractor


def extract_snakemake_workflow(
    pkg_root: Path, snakefile: Path, version: Optional[str] = None
) -> SnakemakeWorkflow:
    extractor = snakemake_workflow_extractor(pkg_root, snakefile, version)
    with extractor:
        dag = extractor.extract_dag()
        wf = SnakemakeWorkflow(dag, version)
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

    wf_spec = get_serializable_workflow(wf, settings, registrable_entity_cache)
    Path("wf_spec.json").write_text(MessageToJson(wf_spec.to_flyte_idl()))

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

    click.secho("\nSerializing workflow entities", bold=True)
    persist_registrable_entities(registrable_entities, output_dir)


def snakefile_path_in_container(snakefile: Path, pkg_root: Path) -> str:
    return str(snakefile.resolve())[len(str(pkg_root.resolve())) + 1 :]


def generate_snakemake_entrypoint(
    wf: SnakemakeWorkflow,
    pkg_root: Path,
    snakefile: Path,
    remote_output_url: Optional[str] = None,
):
    entrypoint_code_block = textwrap.dedent(r"""
        import os
        from pathlib import Path
        import shutil
        import subprocess
        from typing import NamedTuple
        import stat
        from dataclasses import dataclass

        from latch import small_task
        from latch.types.file import LatchFile

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)

        def check_exists_and_rename(old: Path, new: Path):
            if new.exists():
                print(f"A file already exists at {new} and will be overwritten.")
                if new.is_dir():
                    shutil.rmtree(new)
            os.renames(old, new)


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
            snakefile_path_in_container(snakefile, pkg_root), remote_output_url
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
        from typing import List, NamedTuple, Optional, TypedDict
        import hashlib
        from urllib.parse import urljoin

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

        from latch import small_task
        from latch_sdk_gql.execute import execute
        from latch.types.directory import LatchDir
        from latch.types.file import LatchFile

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)

        def check_exists_and_rename(old: Path, new: Path):
            if new.exists():
                print(f"A file already exists at {new} and will be overwritten.")
                if new.is_dir():
                    shutil.rmtree(new)
            os.renames(old, new)

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
        version,
        image_name,
        account_id,
        wf.remote_output_url,
    )

    entrypoint = pkg_root / ".latch" / "snakemake_jit_entrypoint.py"
    entrypoint.write_text(code_block + "\n")

    return entrypoint
