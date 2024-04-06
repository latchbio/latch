import sys
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, get_args

import click
from flyteidl.admin.launch_plan_pb2 import LaunchPlan as _idl_admin_LaunchPlan
from flyteidl.admin.task_pb2 import TaskSpec as _idl_admin_TaskSpec
from flyteidl.admin.workflow_pb2 import WorkflowSpec as _idl_admin_WorkflowSpec
from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core import constants as common_constants
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.interface import Interface
from flytekit.core.node import Node
from flytekit.core.promise import Promise
from flytekit.core.type_engine import TypeEngine, TypeTransformerFailedError
from flytekit.core.utils import _dnsify
from flytekit.core.workflow import WorkflowBase
from flytekit.models import common as common_models
from flytekit.models import interface as interface_models
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import literals as literals_models
from flytekit.models import task as task_models
from flytekit.models import types as type_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.models.core import identifier as identifier_model
from flytekit.models.core import workflow as workflow_model
from flytekit.models.core.workflow import TaskNodeOverrides
from flytekit.tools.serialize_helpers import persist_registrable_entities
from google.protobuf.json_format import MessageToJson
from typing_extensions import TypeAlias

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
            try:
                default_lv = TypeEngine.to_literal(
                    ctx, default, python_type=interface.inputs[k], expected=v.type
                )
            except TypeTransformerFailedError as e:
                click.secho(
                    f"Failed to transform default value for parameter `{k}` to a"
                    f" literal: {str(e)}",
                    fg="red",
                )
                raise click.exceptions.Exit(1) from e

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


RegistrableEntity = Union[
    task_models.TaskSpec,
    launch_plan_models.LaunchPlan,
    admin_workflow_models.WorkflowSpec,
]


def should_register_with_admin(entity: FlyteSerializableModel) -> bool:
    return isinstance(entity, get_args(RegistrableEntity))


def serialize(
    wf: WorkflowBase,
    output_dir: str,
    image_name: str,
    dkr_repo: str,
    *,
    write_spec: bool = False,
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

    click.secho(
        "\nBuilding flyte models: \x1b[?25l",
        nl=False,
    )

    registrable_entities = []
    i = 0
    for x in list(registrable_entity_cache.values()) + [admin_lp]:
        progress_str = f"{i + 1}/{len(registrable_entity_cache) + 1}"

        click.echo("\x1b[0K", nl=False)
        click.secho(progress_str, dim=True, italic=True, nl=False)
        click.echo(f"\x1b[{len(progress_str)}D", nl=False)

        i += 1

        if not should_register_with_admin(x):
            continue

        registrable_entities.append(x.to_flyte_idl())

    click.echo("\x1b[0K", nl=False)
    click.secho("Done. \x1b[?25h", italic=True)

    click.secho("\nSerializing workflow entities", bold=True)

    persist_registrable_entities(registrable_entities, output_dir)

    if not write_spec:
        return

    spec_dir = Path("spec")
    spec_dir.mkdir(parents=True, exist_ok=True)

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


def binding_data_from_python(
    expected_literal_type: type_models.LiteralType,
    t_value: Any,
    t_value_type: Optional[Type] = None,
) -> Optional[literals_models.BindingData]:
    if isinstance(t_value, Promise):
        if not t_value.is_ready:
            return literals_models.BindingData(promise=t_value.ref)


def binding_from_python(
    var_name: str,
    expected_literal_type: type_models.LiteralType,
    t_value: Any,
    t_value_type: Type,
) -> literals_models.Binding:
    binding_data = binding_data_from_python(
        expected_literal_type, t_value, t_value_type
    )
    return literals_models.Binding(var=var_name, binding=binding_data)
