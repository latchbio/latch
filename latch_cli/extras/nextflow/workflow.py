from pathlib import Path
from typing import List

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
from latch.types.metadata import NextflowFileParameter

from .dag import DAG


class NextflowWorkflow(WorkflowBase, ClassStorageTaskResolver):
    def __init__(self, nf_script: Path, dag: DAG):
        assert metadata._nextflow_metadata is not None
        assert metadata._nextflow_metadata.output_directory is not None

        self.output_directory = metadata._nextflow_metadata.output_directory

        docstring = Docstring(
            f"{metadata._nextflow_metadata.display_name}\n\nSample Description\n\n"
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

        self.flags_to_params = {
            f"--{k}": (
                f"wf_paths[wf_{k}]"
                if isinstance(v, NextflowFileParameter)
                else f"wf_{k}"
            )
            for k, v in metadata._nextflow_metadata.parameters.items()
        }

        self.downloadable_params = {
            k: str(v.path)
            for k, v in metadata._nextflow_metadata.parameters.items()
            if isinstance(v, NextflowFileParameter) and v._download
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

        self.dag = dag
        self.nf_script = nf_script

    def execute(self, **kwargs):
        return exception_scopes.user_entry_point(self._workflow_function)(**kwargs)
