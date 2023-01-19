import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile, LatchOutputDir
from latch.functions.messages import message


@small_task
def sort_bam_task(sam: LatchFile, output_directory: LatchOutputDir) -> LatchFile:

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

    try:
        # best practice arguments for subprocess.run
        subprocess.run(_samtools_sort_cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        # will display in the messages tab of the execution graph for the sort_bam_task node
        message(
            "error",
            {"title": "Samtools Sort Failed", "body": f"Error: {str(e)}"},
        )
        raise e

    # intended output path of the file in Latch console, constructed from
    # the user provided output directory
    output_location = f"{output_directory.remote_directory}/covid_sorted.bam"

    return LatchFile(str(bam_file), output_location)
