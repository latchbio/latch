"""
Minimal template workflow to show how to run workflows that use conda in Latch

For a more comprehensive template, see the assemble_and_sort workflow
For examples on how to use conda in Latch, see https://docs.latch.bio/examples/workflows_examples.html
"""

from latch import small_task, workflow
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter


# change the name of this function to something more descriptive
@small_task
def conda_task(input_file: LatchFile) -> LatchFile:
    ...


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
    },
    tags=[],
)


# change the name of this function to something more descriptive
@workflow(metadata)
def conda_workflow(input_file: LatchFile) -> LatchFile:
    """Description...

    markdown header
    ----

    Write some documentation about your workflow in
    markdown here:

    > Regular markdown constructs work as expected.

    # Heading

    * content1
    * content2
    """
    return conda_task(input_file=input_file)
