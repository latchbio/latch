"""
Assemble and sort some COVID reads...
"""
from wf.assemble import assembly_task
from wf.sort import sort_bam_task

from latch.resources.launch_plan import LaunchPlan
from latch.resources.workflow import workflow
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile
from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter, LatchRule

"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="Assemble and Sort FastQ Files",
    documentation="your-docs.dev",
    author=LatchAuthor(
        name="Author",
        email="author@gmail.com",
        github="github.com/author",
    ),
    repository="https://github.com/your-repo",
    license="MIT",
    parameters={
        "read1": LatchParameter(
            display_name="Read 1",
            description="Paired-end read 1 file to be assembled.",
            batch_table_column=True,  # Show this parameter in batched mode.
            rules=[
                # validate the input file using regex
                LatchRule(
                    regex="(.fastq|.fastq.gz|.fq|.fq.gz)$",
                    message="Only fastq, fastq.gz, fq, fq.gz extensions are valid",
                )
            ],
        ),
        "read2": LatchParameter(
            display_name="Read 2",
            description="Paired-end read 2 file to be assembled.",
            batch_table_column=True,  # Show this parameter in batched mode.
            rules=[
                LatchRule(
                    regex="(.fastq|.fastq.gz|.fq|.fq.gz)$",
                    message="Only fastq, fastq.gz, fq, fq.gz extensions are valid",
                )
            ],
        ),
        "output_directory": LatchParameter(
            display_name="Output Directory",
            description="Where to place the result file.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
    tags=[],
)


@workflow(metadata)
def assemble_and_sort(
    read1: LatchFile, read2: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:
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
    sam = assembly_task(read1=read1, read2=read2, output_directory=output_directory)
    return sort_bam_task(sam=sam, output_directory=output_directory)


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
        "output_directory": LatchOutputDir("latch:///assemble_and_sort_outputs"),
    },
)
