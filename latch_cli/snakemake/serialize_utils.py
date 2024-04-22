import re
from pathlib import Path
from typing import Dict, Union

from flytekit import LaunchPlan
from flytekit.configuration import SerializationSettings
from flytekit.core import constants as common_constants
from flytekit.core.base_task import PythonTask
from flytekit.core.node import Node
from flytekit.core.utils import _dnsify
from flytekit.core.workflow import WorkflowBase
from flytekit.models import common as common_models
from flytekit.models import interface as interface_models
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.models.core import identifier as identifier_model
from flytekit.models.core import workflow as workflow_model
from flytekit.models.core.workflow import TaskNodeOverrides
from typing_extensions import TypeAlias

from latch_cli.utils import urljoins

FlyteLocalEntity: TypeAlias = Union[
    PythonTask,
    Node,
    LaunchPlan,
    WorkflowBase,
]

FlyteSerializableModel: TypeAlias = Union[
    task_models.TaskSpec,
    workflow_model.Node,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]

EntityCache: TypeAlias = Dict[FlyteLocalEntity, FlyteSerializableModel]


def get_serializable_launch_plan(
    entity: LaunchPlan,
    settings: SerializationSettings,
    cache: EntityCache,
) -> launch_plan_models.LaunchPlan:
    if entity in cache:
        return cache[entity]

    wf_id = identifier_model.Identifier(
        resource_type=identifier_model.ResourceType.WORKFLOW,
        project=settings.project,
        domain=settings.domain,
        name=entity.workflow.name,
        version=settings.version,
    )

    lps = launch_plan_models.LaunchPlanSpec(
        workflow_id=wf_id,
        entity_metadata=launch_plan_models.LaunchPlanMetadata(
            schedule=entity.schedule,
            notifications=entity.notifications,
        ),
        default_inputs=entity.parameters,
        fixed_inputs=entity.fixed_inputs,
        labels=common_models.Labels({}),
        annotations=(
            entity.annotations
            if entity.annotations is not None
            else common_models.Annotations({})
        ),
        auth_role=None,
        raw_output_data_config=(
            entity.raw_output_data_config
            if entity.raw_output_data_config is not None
            else common_models.RawOutputDataConfig("")
        ),
        max_parallelism=entity.max_parallelism,
        security_context=entity.security_context,
    )

    lp_id = identifier_model.Identifier(
        resource_type=identifier_model.ResourceType.LAUNCH_PLAN,
        project=settings.project,
        domain=settings.domain,
        name=entity.name,
        version=settings.version,
    )
    lp_model = launch_plan_models.LaunchPlan(
        id=lp_id,
        spec=lps,
        closure=launch_plan_models.LaunchPlanClosure(
            state=None,
            expected_inputs=interface_models.ParameterMap({}),
            expected_outputs=interface_models.VariableMap({}),
        ),
    )
    cache[entity] = lp_model

    return lp_model


def get_serializable_task(
    entity: FlyteLocalEntity,
    settings: SerializationSettings,
    cache: EntityCache,
) -> task_models.TaskSpec:
    if entity in cache:
        return cache[entity]

    task_id = identifier_model.Identifier(
        identifier_model.ResourceType.TASK,
        settings.project,
        settings.domain,
        entity.name,
        settings.version,
    )

    container = entity.get_container(settings)
    pod = entity.get_k8s_pod(settings)

    tt = task_models.TaskTemplate(
        id=task_id,
        type=entity.task_type,
        metadata=entity.metadata.to_taskmetadata_model(),
        interface=entity.interface,
        custom=entity.get_custom(settings),
        container=container,
        task_type_version=entity.task_type_version,
        security_context=entity.security_context,
        config=entity.get_config(settings),
        k8s_pod=pod,
        sql=entity.get_sql(settings),
    )
    task_model = task_models.TaskSpec(template=tt)
    cache[entity] = task_model
    return task_model


class SerializationError(Exception):
    pass


def get_serializable_node(
    entity: Node,
    settings: SerializationSettings,
    cache: EntityCache,
) -> workflow_model.Node:
    if entity in cache:
        return cache[entity]

    if entity.flyte_entity is None:
        raise SerializationError(
            f"SnakemakeWorkflow Node {entity.id} has no task and cannot be serialized."
        )

    upstream_sdk_nodes = [
        get_serializable_node(n, settings, cache)
        for n in entity.upstream_nodes
        if n.id != common_constants.GLOBAL_INPUT_NODE_ID
    ]

    if isinstance(entity.flyte_entity, PythonTask):
        task_spec = get_serializable_task(entity.flyte_entity, settings, cache)
        node_model = workflow_model.Node(
            id=_dnsify(entity.id),
            metadata=entity.metadata,
            inputs=entity.bindings,
            upstream_node_ids=[n.id for n in upstream_sdk_nodes],
            output_aliases=[],
            task_node=workflow_model.TaskNode(
                reference_id=task_spec.template.id,
                overrides=TaskNodeOverrides(resources=entity._resources),
            ),
        )
        cache[entity] = node_model
        return node_model
    else:
        raise SerializationError(
            "Cannot serialize a SnakemakeWorkflow node {entity.id} containing a task"
            f" of type {entity._flyte_entity}.The task must be a PythonTask."
        )


def get_serializable_workflow(
    entity: WorkflowBase,
    settings: SerializationSettings,
    cache: EntityCache,
) -> admin_workflow_models.WorkflowSpec:
    if entity in cache:
        return cache[entity]

    upstream_node_models = [
        get_serializable_node(n, settings, cache)
        for n in entity.nodes
        if n.id != common_constants.GLOBAL_INPUT_NODE_ID
    ]

    wf_id = identifier_model.Identifier(
        resource_type=identifier_model.ResourceType.WORKFLOW,
        project=settings.project,
        domain=settings.domain,
        name=entity.name,
        version=settings.version,
    )
    wf_t = workflow_model.WorkflowTemplate(
        id=wf_id,
        metadata=entity.workflow_metadata.to_flyte_model(),
        metadata_defaults=entity.workflow_metadata_defaults.to_flyte_model(),
        interface=entity.interface,
        nodes=upstream_node_models,
        outputs=entity.output_bindings,
    )

    admin_wf = admin_workflow_models.WorkflowSpec(template=wf_t, sub_workflows=[])
    cache[entity] = admin_wf
    return admin_wf


def update_mapping(cur: Path, stem: Path, remote: str, mapping: Dict[str, str]):
    if cur.is_dir():
        for p in cur.iterdir():
            update_mapping(p, stem / p.name, urljoins(remote, p.name), mapping)
    mapping[str(stem)] = remote
