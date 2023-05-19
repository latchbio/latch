"""
Minimal template workflow to show how to run workflows that use nextflow in Latch

For a more comprehensive template, see the assemble_and_sort workflow
For examples on how to use nextflow in Latch, see https://docs.latch.bio/examples/workflows_examples.html
"""
from typing import List, Union

from wf.task import run_nfcore_fetchngs

from latch import workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import (
    LatchAuthor,
    LatchFile,
    LatchMetadata,
    LatchOutputDir,
    LatchParameter,
)
from latch.types.directory import LatchDir

"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="nf-core/fetchngs",
    documentation="https://nf-co.re/fetchngs/1.8/usage",
    author=LatchAuthor(
        name="nf-core",
    ),
    parameters={
        "ids": LatchParameter(
            display_name="IDs CSV",
            description="CSV file containing ids from SRA, ENA, DDBJ or Synapse",
            batch_table_column=True,  # Show this parameter in batched mode
        ),
        "output_directory": LatchParameter(
            display_name="Output Directory",
            description="Where to place the result file.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
)


@workflow(metadata)
def nfcore_fetchngs(ids: LatchFile, output_directory: LatchOutputDir) -> List[Union[LatchFile, LatchDir]]:
    return run_nfcore_fetchngs(ids=ids, output_directory=output_directory)


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    nfcore_fetchngs,
    "Test Data",
    {
        "ids": LatchFile("s3://latch-public/init/nfcore_example_ids.csv"),
        "output_directory": LatchOutputDir("latch:///nfcore_fetchngs_output"),
    },
)
