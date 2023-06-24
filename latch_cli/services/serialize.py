import os
import textwrap
from itertools import chain, filterfalse
from pathlib import Path
from typing import List, Union

from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import literals as literals_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.tools.serialize_helpers import persist_registrable_entities
from snakemake.common import ON_WINDOWS
from snakemake.dag import DAG
from snakemake.executors import RealExecutor
from snakemake.persistence import Persistence
from snakemake.rules import Rule
from snakemake.workflow import Workflow

from latch_cli.centromere.ctx import _CentromereCtx
from latch_cli.snakemake.serialize_utils import (
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from latch_cli.snakemake.workflow import SnakemakeWorkflow, interface_to_parameters

RegistrableEntity = Union[
    task_models.TaskSpec,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]


def should_register_with_admin(entity) -> bool:
    return isinstance(
        entity,
        (
            task_models.TaskSpec,
            launch_plan_models.LaunchPlan,
            admin_workflow_models.WorkflowSpec,
        ),
    )


class SnakemakeWorkflowExtractor(Workflow):
    def __init__(self, snakefile):
        super().__init__(snakefile=snakefile)

    def extract(self):
        def rules(items):
            return map(self._rules.__getitem__, filter(self.is_rule, items))

        def files(items):
            relpath = (
                lambda f: f
                if os.path.isabs(f) or f.startswith("root://")
                else os.path.relpath(f)
            )
            return map(relpath, filterfalse(self.is_rule, items))

        # if not targets and not target_jobs:
        targets = [self.default_target] if self.default_target is not None else list()

        prioritytargets = list()
        forcerun = list()
        until = list()
        omit_from = list()

        priorityrules = set(rules(prioritytargets))
        priorityfiles = set(files(prioritytargets))
        forcerules = set(rules(forcerun))
        forcefiles = set(files(forcerun))
        untilrules = set(rules(until))
        untilfiles = set(files(until))
        omitrules = set(rules(omit_from))
        omitfiles = set(files(omit_from))
        targetrules = set(
            chain(
                rules(targets),
                filterfalse(Rule.has_wildcards, priorityrules),
                filterfalse(Rule.has_wildcards, forcerules),
                filterfalse(Rule.has_wildcards, untilrules),
            )
        )
        targetfiles = set(chain(files(targets), priorityfiles, forcefiles, untilfiles))

        if ON_WINDOWS:
            targetfiles = set(tf.replace(os.sep, os.altsep) for tf in targetfiles)

        rules = self.rules

        dag = DAG(
            self,
            rules,
            targetfiles=targetfiles,
            targetrules=targetrules,
            forcefiles=forcefiles,
            forcerules=forcerules,
            priorityfiles=priorityfiles,
            priorityrules=priorityrules,
            untilfiles=untilfiles,
            untilrules=untilrules,
            omitfiles=omitfiles,
            omitrules=omitrules,
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
    dag = workflow.extract()

    wf_name = "snakemake_wf"
    wf = SnakemakeWorkflow(
        wf_name,
        dag,
    )
    wf.compile()
    return wf_name, wf


def get_snakefile(pkg_root: Path) -> Path:
    return Path("Snakefile")


def serialize(pkg_root: Path, output_dir: Path, dkr_repo: str, version: str):
    """Serializes workflow code into lyteidl protobuf.

    Args:
        pkg_root: The directory of project with workflow code to be serialized
        output_dir: The directory where generated protobuf will go
    """

    pkg_root = Path(pkg_root).resolve()
    snakefile = get_snakefile(pkg_root)

    wf_name, wf = extract_snakemake_workflow(snakefile)

    default_img = Image(
        name=wf_name,
        fqn=f"{dkr_repo}/{wf_name}",
        tag=version,
    )
    settings = SerializationSettings(
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    registrable_entity_cache = {}

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


def generate_snakemake_entrypoint(wf: SnakemakeWorkflow, ctx: _CentromereCtx):

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
    entrypoint = ctx.pkg_root.joinpath(".latch/latch_entrypoint.py")
    snakefile = get_snakefile(ctx.pkg_root)

    # TODO - pull out what we need from RealExecutor
    workflow = SnakemakeWorkflowExtractor(
        snakefile=snakefile,
    )
    workflow.include(
        snakefile,
        overwrite_default_target=True,
    )
    dag = workflow.extract()
    executor = RealExecutor(workflow, dag)
    executor.cores = 8

    for task in wf.snakemake_tasks:
        entrypoint_code_block += task.get_fn_code(executor)

    with open(entrypoint, "w") as f:
        f.write(entrypoint_code_block)
