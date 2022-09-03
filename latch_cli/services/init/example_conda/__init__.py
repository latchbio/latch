"""
Minimal template workflow to show how to run workflows that use conda in Latch

For a more comprehensive template, see the assemble_and_sort workflow
"""

from latch import small_task, workflow
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter


# change the name of this function to something more descriptive
@small_task
def conda_task(input_file: LatchFile) -> LatchFile:
    ...


"""Minimal metadata object - fill in fields with your own values"""
metadata = LatchMetadata(
    display_name="",
    # documentation="",
    author=LatchAuthor(
        name="",
        email="",
        github="",
    ),
    # repository="",
    # license="",
    parameters={
        "input_file": LatchParameter(
            display_name="Input File",
            # description="",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
    # tags=[],
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
