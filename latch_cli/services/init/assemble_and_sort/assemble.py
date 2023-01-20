import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile, LatchOutputDir
from latch.functions.messages import message


@small_task
def assembly_task(read1: LatchFile, read2: LatchFile, output_directory: LatchOutputDir) -> LatchFile:

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

    try:
        # best practice arguments for subprocess.run
        subprocess.run(_bowtie2_cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        # will display in the messages tab of the execution graph for the assembly_task node
        message(
            "error",
            {"title": "Bowtie2 Failed", "body": f"Error: {str(e)}"},
        )
        raise e

    # intended output path of the file in Latch console, constructed from
    # the user provided output directory
    output_location = f"{output_directory.remote_directory}/covid_assembly.sam"

    return LatchFile(str(sam_file), output_location)
