"""
Minimal template workflow to show how to run workflows that use conda in Latch

For a more comprehensive template, see the assemble_and_sort workflow
For examples on how to use conda in Latch, see https://docs.latch.bio/examples/workflows_examples.html
"""

from wf.task import blastp_task

from latch import workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import (
    LatchAuthor,
    LatchFile,
    LatchMetadata,
    LatchOutputDir,
    LatchParameter,
)

metadata = LatchMetadata(
    display_name="biocontainers/blastp",
    author=LatchAuthor(
        name="biocontainers",
    ),
    parameters={
        "query_file": LatchParameter(
            display_name="Input FASTA File",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "database": LatchParameter(
            display_name="Database to run blastp against (.faa file)",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "output_directory": LatchParameter(
            display_name="Output Directory",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
    tags=[],
)


@workflow(metadata)
def blast_wf(
    query_file: LatchFile, database: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:
    return blastp_task(
        query_fasta=query_file, database=database, output_directory=output_directory
    )


LaunchPlan(
    blast_wf,
    "Test Data",
    {
        "query_file": LatchFile(
            "s3://latch-public/test-data/1534/host-data/P04156.fasta"
        ),
        "database": LatchFile(
            "s3://latch-public/test-data/1534/host-data/zebrafish.1.protein.faa"
        ),
        "output_directory": LatchOutputDir("latch:///biocontainers_blastp_output"),
    },
)
