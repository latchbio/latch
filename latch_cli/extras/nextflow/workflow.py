from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, Type

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
from latch_cli.extras.common.utils import is_blob_type, is_downloadable_blob_type

from .dag import DAG

# def _get_flattened_inputs(
#     key: str, t: Type, val: Any, inputs: Dict[str, Tuple[Type, Any]]
# ):
#     if not is_dataclass(t):
#         inputs[key] = (t, val)
#         return

#     for f in fields(t):
#         v = val
#         if val is not None:
#             v = getattr()


def _get_flags_to_params(key: str, t: Type, flags: Dict[str, str]):
    if is_blob_type(t):
        flags[f"--{key}"] = f"wf_paths['wf_{key}']"
    elif is_dataclass(t):
        for f in fields(t):
            _get_flags_to_params(f"{key}.{f.name}", f.type, flags)
    else:
        flags[f"--{key}"] = f"wf_{key}"


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

        self.flattened_inputs = {}

        self.flags_to_params = {}
        for k, v in metadata._nextflow_metadata.parameters.items():
            assert v.type is not None
            _get_flags_to_params(k, v.type, self.flags_to_params)

        self.downloadable_params = {
            k
            for k, v in metadata._nextflow_metadata.parameters.items()
            if is_downloadable_blob_type(v.type)
        }

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
