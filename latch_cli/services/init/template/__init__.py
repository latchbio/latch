"""
Minimal template workflow to show the structure of a Latch workflow

For a more comprehensive example, see the assemble_and_sort workflow
For examples on how to use the Latch SDK, see https://docs.latch.bio/examples/workflows_examples.html
"""

from wf.task import task

from latch import workflow
from latch.types import (
    LatchAuthor,
    LatchFile,
    LatchMetadata,
    LatchOutputDir,
    LatchParameter,
)

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
def latch_workflow(
    input_file: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:
    return task(input_file=input_file, output_directory=output_directory)
