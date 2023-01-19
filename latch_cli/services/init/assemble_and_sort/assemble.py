import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile
from latch.functions.messages import message


@small_task
def assembly_task(read1: LatchFile, read2: LatchFile) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    _bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "-bt2-idx",
        "wuhan",
        "-m1",
        read1.local_path,
        "-m2",
        read2.local_path,
        "-sam",
        str(sam_file),
        "--very-sensitive-local",
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

    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")
