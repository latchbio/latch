import hashlib
import importlib
import json
import re
import textwrap
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
)
from urllib.parse import urlparse

import click
import snakemake
import snakemake.io
import snakemake.jobs
from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core import constants as common_constants
from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.docstring import Docstring
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise
from flytekit.core.python_auto_container import (
    DefaultTaskResolver,
    PythonAutoContainerTask,
)
from flytekit.core.type_engine import TypeEngine
from flytekit.core.utils import _dnsify
from flytekit.core.workflow import (
    WorkflowBase,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models import common as common_models
from flytekit.models import interface as interface_models
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import literals as literals_models
from flytekit.models import task as _task_models
from flytekit.models import task as task_models
from flytekit.models import types as type_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.models.core import identifier as identifier_model
from flytekit.models.core import workflow as workflow_model
from flytekit.models.core.types import BlobType
from flytekit.models.core.workflow import TaskNodeOverrides
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralMap, Scalar
from flytekit.tools.serialize_helpers import persist_registrable_entities
from flytekitplugins.pod.task import (
    _PRIMARY_CONTAINER_NAME_FIELD,
    Pod,
    _sanitize_resource_name,
)
from kubernetes.client import ApiClient
from kubernetes.client.models import V1Container, V1EnvVar, V1ResourceRequirements
from snakemake.dag import DAG
from snakemake.jobs import GroupJob, Job
from typing_extensions import TypeAlias, TypedDict

from latch_cli.utils import urljoins

RegistrableEntity = Union[
    task_models.TaskSpec,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]


def should_register_with_admin(entity: RegistrableEntity) -> bool:
    return isinstance(entity, get_args(RegistrableEntity))


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

    params: Dict[str, interface_models.Parameter] = {}
    for k, v in inputs_vars.items():
        val, default = interface.inputs_with_defaults[k]
        required = default is None
        default_lv = None

        ctx = FlyteContextManager.current_context()
        if default is not None:
            default_lv = TypeEngine.to_literal(
                ctx, default, python_type=interface.inputs[k], expected=v.type
            )

        params[k] = interface_models.Parameter(
            var=v, default=default_lv, required=required
        )
    return interface_models.ParameterMap(params)


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


def serialize_jit_register_workflow(
    jit_wf: WorkflowBase,
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
