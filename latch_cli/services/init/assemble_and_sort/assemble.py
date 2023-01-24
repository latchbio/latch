import subprocess
from pathlib import Path

from latch import small_task
from latch.functions.messages import message
from latch.types import LatchFile, LatchOutputDir


@small_task
def assembly_task(
    read1: LatchFile, read2: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    _bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "--very-sensitive-local",
        "-x",
        "wuhan",
        "-1",
        read1.local_path,
        "-2",
        read2.local_path,
        "-S",
        str(sam_file),
    ]

    try:
        # We use shell=True for all the benefits of pipes and other shell features.
        # When using shell=True, we pass the entire command as a single string as
        # opposed to a list since the shell will parse the string into a list
        # using its own rules.
        subprocess.run(" ".join(_bowtie2_cmd), shell=True, check=True)
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
