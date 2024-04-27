from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Tuple, Type, TypeVar, Union, get_args, get_origin

import click
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.docstring import Docstring
from flytekit.core.interface import Interface
from flytekit.core.workflow import (
    WorkflowBase,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.exceptions import scopes as exception_scopes

from latch.types import metadata
from latch.types.file import LatchFile
from latch_cli.extras.common.utils import (
    is_blob_type,
    is_downloadable_blob_type,
    is_output_dir,
)

from .dag import DAG


def get_flags(wf_paths: Dict[str, Path], **kwargs) -> List[str]:
    assert metadata._nextflow_metadata is not None

    flags = []
    for key in kwargs.keys():
        param = metadata._nextflow_metadata.parameters.get(key)
        assert param is not None, f"Param {key} is not a workflow parameter"

        t = param.type
        if param.samplesheet:
            t = LatchFile
            wf_paths[f"wf_{key}"] = kwargs[key]

        v = kwargs[key]

        _add_flags(key, t, v, flags, wf_paths)

    return flags


T = TypeVar("T")


def _add_flags(key: str, t: Type[T], v: T, flags: List[str], wf_paths: Dict[str, Path]):
    if v is None or is_output_dir(t):
        return

    if get_origin(t) is Union:
        for arg in get_args(t):
            if isinstance(v, arg):
                _add_flags(key, arg, v, flags, wf_paths)
                return

    if t is bool:
        if v:
            flags.append(f"--{key}")
    elif is_blob_type(t):
        flags.extend([f"--{key}", str(wf_paths[f"wf_{key}"])])
    elif is_dataclass(t):
        for f in fields(t):
            _add_flags(f"{key}.{f.name}", f.type, getattr(v, f.name), flags, wf_paths)
    elif issubclass(t, Enum):
        flags.extend([f"--{key}", getattr(v, "value")])
    else:
        flags.extend([f"--{key}", str(v)])


class NextflowWorkflow(WorkflowBase, ClassStorageTaskResolver):
    def __init__(self, pkg_root: Path, nf_script: Path, version: str, dag: DAG):
        assert metadata._nextflow_metadata is not None
        assert metadata._nextflow_metadata.output_directory is not None

        self.output_directory = metadata._nextflow_metadata.output_directory
        self.docker_metadata = metadata._nextflow_metadata.docker_metadata
        md_path = metadata._nextflow_metadata.about_page_markdown

        if md_path is not None and md_path.exists():
            click.secho(f"Rendering workflow About page content from {md_path}")
            markdown_content = md_path.read_text()
        else:
            markdown_content = (
                "Add markdown content to a file and add the path",
                "to your workflow metadata to populate this page.\nMore information",
                "[here](https://wiki.latch.bio/docs/nextflow/quickstart).",
            )

        docstring = Docstring(
            f"{metadata._nextflow_metadata.display_name}\n\n{markdown_content}\n\n"
            + str(metadata._nextflow_metadata)
        )
        python_interface = Interface(
            {
                k: (v.type, v.default) if v.default is not None else v.type
                for k, v in metadata._nextflow_metadata.parameters.items()
                if v.type is not None
            },
            {},
            docstring=docstring,
        )

        self.downloadable_params = {
            k
            for k, v in metadata._nextflow_metadata.parameters.items()
            if is_downloadable_blob_type(v.type)
        }

        self.publish_dir_param = None
        for k, v in metadata._nextflow_metadata.parameters.items():
            if is_output_dir(v.type):
                if self.publish_dir_param is not None:
                    click.secho(
                        "Only one LatchOutputDir parameter is allowed.", fg="red"
                    )
                    raise click.exceptions.Exit(1)
                self.publish_dir_param = f"wf_{k}"

        name = metadata._nextflow_metadata.name
        assert name is not None

        super().__init__(
            name=name,
            workflow_metadata=WorkflowMetadata(
                on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY
            ),
            workflow_metadata_defaults=WorkflowMetadataDefaults(False),
            python_interface=python_interface,
        )

        from .tasks.base import NextflowBaseTask

        self.nextflow_tasks: List[NextflowBaseTask] = []

        self.pkg_root = pkg_root
        self.nf_script = nf_script
        self.version = version
        self.dag = dag

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)
