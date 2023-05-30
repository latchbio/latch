import os
import textwrap
from itertools import chain, filterfalse
from pathlib import Path
from typing import List, Set, Union, get_args

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

from latch_cli.centromere.ctx import _CentromereCtx
from latch_cli.snakemake.serialize_utils import (
    EntityCache,
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from latch_cli.snakemake.workflow import SnakemakeWorkflow, interface_to_parameters

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


def extract_snakemake_workflow(snakefile: Path) -> (str, SnakemakeWorkflow):
    workflow = SnakemakeWorkflowExtractor(
        snakefile=snakefile,
    )
    workflow.include(
        snakefile,
        overwrite_default_target=True,
    )
    dag = workflow.extract_dag()

    wf_name = "snakemake_wf"
    wf = SnakemakeWorkflow(
        wf_name,
        dag,
    )
    wf.compile()
    return wf_name, wf


def serialize_snakemake(
    pkg_root: Path,
    snakefile: Path,
    output_dir: Path,
    image_name: str,
    dkr_repo: str,
):
    pkg_root = Path(pkg_root).resolve()
    _, wf = extract_snakemake_workflow(snakefile)

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
    admin_lp = get_serializable_launch_plan(settings, lp, registrable_entity_cache)

    registrable_entities = [
        x.to_flyte_idl()
        for x in list(
            filter(should_register_with_admin, list(registrable_entity_cache.values()))
        )
        + [admin_lp]
    ]
    persist_registrable_entities(registrable_entities, output_dir)


def generate_snakemake_entrypoint(
    wf: SnakemakeWorkflow, ctx: _CentromereCtx, snakefile: Path
):
    entrypoint_code_block = textwrap.dedent("""\
           import subprocess
           from pathlib import Path
           from typing import NamedTuple

           from latch import small_task
           from latch.types import LatchFile

           def ensure_parents_exist(path: Path):
               path.parent.mkdir(parents=True, exist_ok=True)
               return path
           """)
    for task in wf.snakemake_tasks:
        snakefile_path_in_container = str(snakefile.resolve())[
            len(str(ctx.pkg_root.resolve())) + 1 :
        ]
        entrypoint_code_block += task.get_fn_code(snakefile_path_in_container)

    entrypoint = ctx.pkg_root.joinpath(".latch/latch_entrypoint.py")
    with open(entrypoint, "w") as f:
        f.write(entrypoint_code_block)
