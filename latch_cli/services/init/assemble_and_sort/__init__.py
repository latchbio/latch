"""
Assemble and sort some COVID reads...
"""

import subprocess
from pathlib import Path

from latch import small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter


@small_task
def assembly_task(read1: LatchFile, read2: LatchFile) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    _bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "-x",
        "wuhan",
        "-1",
        read1.local_path,
        "-2",
        read2.local_path,
        "--very-sensitive-local",
        "-S",
        str(sam_file),
    ]

    subprocess.run(_bowtie2_cmd)

    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")


@small_task
def sort_bam_task(sam: LatchFile) -> LatchFile:

    bam_file = Path("covid_sorted.bam").resolve()

    _samtools_sort_cmd = [
        "samtools",
        "sort",
        "-o",
        str(bam_file),
        "-O",
        "bam",
        sam.local_path,
    ]

    subprocess.run(_samtools_sort_cmd)

    return LatchFile(str(bam_file), "latch:///covid_sorted.bam")


"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="Assemble and Sort FastQ Files",
    documentation="your-docs.dev",
    author=LatchAuthor(
        name="John von Neumann",
        email="hungarianpapi4@gmail.com",
        github="github.com/fluid-dynamix",
    ),
    repository="https://github.com/your-repo",
    license="MIT",
    parameters={
        "read1": LatchParameter(
            display_name="Read 1",
            description="Paired-end read 1 file to be assembled.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "read2": LatchParameter(
            display_name="Read 2",
            description="Paired-end read 2 file to be assembled.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
    tags=[],
)


@workflow(metadata)
def assemble_and_sort(read1: LatchFile, read2: LatchFile) -> LatchFile:
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
    sam = assembly_task(read1=read1, read2=read2)
    return sort_bam_task(sam=sam)


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    assemble_and_sort,
    "Test Data",
    {
        "read1": LatchFile("s3://latch-public/init/r1.fastq"),
        "read2": LatchFile("s3://latch-public/init/r2.fastq"),
    },
)
