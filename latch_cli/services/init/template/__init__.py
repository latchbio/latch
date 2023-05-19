from wf.task import task

from latch import workflow
from latch.types import (
    LatchAuthor,
    LatchFile,
    LatchMetadata,
    LatchOutputDir,
    LatchParameter,
)

metadata = LatchMetadata(
    display_name="WF_NAME",
    author=LatchAuthor(
        name="AUTHOR_NAME",
    ),
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
)


@workflow(metadata)
def template_workflow(
    input_file: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:
    return task(input_file=input_file, output_directory=output_directory)
