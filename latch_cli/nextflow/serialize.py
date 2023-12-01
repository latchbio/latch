import importlib
import json
import os
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar
from urllib.parse import urlparse

import click
from flytekit import LaunchPlan
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core import constants as _common_constants
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.constants import SdkTaskType
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.docstring import Docstring
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.map_task import MapPythonTask
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise
from flytekit.core.python_auto_container import (
    DefaultTaskResolver,
    PythonAutoContainerTask,
)
from flytekit.core.workflow import (
    WorkflowBase,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models import literals as literals_models
from flytekit.models.interface import Variable
from flytekit.models.literals import Literal
from flytekit.tools.serialize_helpers import persist_registrable_entities
from flytekitplugins.pod.task import Pod

from latch.resources.tasks import custom_task
from latch.types import metadata
from latch.types.metadata import ParameterType
from latch_cli.nextflow.jit import NFJITRegisterWorkflow
from latch_cli.nextflow.workflow import NextflowProcessTask, NextflowWorkflow
from latch_cli.services.register.register import (
    _print_reg_resp,
    _recursive_list,
    register_serialized_pkg,
)
from latch_cli.snakemake.serialize import should_register_with_admin
from latch_cli.snakemake.serialize_utils import (
    EntityCache,
    get_serializable_launch_plan,
    get_serializable_workflow,
)
from latch_cli.snakemake.workflow import binding_from_python, interface_to_parameters


def serialize_nf_jit_register_workflow(
    jit_wf: NFJITRegisterWorkflow,
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


def generate_nf_entrypoint(
    wf: NextflowWorkflow,
    pkg_root: Path,
    remote_output_url: Optional[str] = None,
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
    """).lstrip()

    for task in wf.nextflow_tasks:
        if isinstance(task, NextflowProcessTask):
            entrypoint_code_block += (
                task.container_task.get_fn_code(remote_output_url) + "\n\n"
            )
        else:
            entrypoint_code_block += task.get_fn_code(remote_output_url) + "\n\n"

    entrypoint = pkg_root / "latch_entrypoint.py"
    entrypoint.write_text(entrypoint_code_block + "\n")
