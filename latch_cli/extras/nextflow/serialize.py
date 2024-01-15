import click
from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models import literals as literals_models
from flytekit.tools.serialize_helpers import persist_registrable_entities

from latch_cli.extras.nextflow.workflow import NextflowWorkflow
from latch_cli.extras.snakemake.serialize import should_register_with_admin
from latch_cli.extras.snakemake.serialize_utils import (
    EntityCache,
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from latch_cli.extras.snakemake.workflow import interface_to_parameters


def serialize_nf(
    nf_wf: NextflowWorkflow,
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

    get_serializable_workflow(nf_wf, settings, registrable_entity_cache)

    parameter_map = interface_to_parameters(nf_wf.python_interface)
    lp = LaunchPlan(
        name=nf_wf.name,
        workflow=nf_wf,
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
