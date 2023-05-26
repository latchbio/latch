import subprocess
from pathlib import Path

from latch.functions.messages import message
from latch.resources.tasks import small_task
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile


@small_task
def sort_bam_task(sam: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    bam_file = Path("covid_sorted.bam").resolve()

    samtools_sort_cmd = [
        "samtools",
        "sort",
        "-o",
        str(bam_file),
        "-O",
        "bam",
        sam.local_path,
    ]

    try:
        # We use shell=True for all the benefits of pipes and other shell features.
        # When using shell=True, we pass the entire command as a single string as
        # opposed to a list since the shell will parse the string into a list
        # using its own rules.
        subprocess.run(" ".join(samtools_sort_cmd), shell=True, check=True)
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
