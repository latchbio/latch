"""
Minimal template workflow to show how to run workflows that use conda in Latch

For a more comprehensive template, see the assemble_and_sort workflow
For examples on how to use conda in Latch, see https://docs.latch.bio/examples/workflows_examples.html
"""

from wf.conda_task import conda_task

from latch.resources.tasks import small_task
from latch.resources.workflow import workflow
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile
from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter

"""Minimal metadata object - fill in fields with your own values"""
metadata = LatchMetadata(
    display_name="CHANGE ME",
    documentation="CHANGE ME",
    author=LatchAuthor(
        name="CHANGE ME",
        email="CHANGE ME",
        github="CHANGE ME",
    ),
    repository="CHANGE ME",
    license="CHANGE ME",
    parameters={
        "input_file": LatchParameter(
            display_name="Input File",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "output_directory": LatchParameter(
            display_name="Output Directory",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
    tags=[],
)


# change the name of this function to something more descriptive
@workflow(metadata)
def conda_workflow(
    input_file: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:
    return conda_task(input_file=input_file, output_directory=output_directory)
