import subprocess
from typing import List

from latch.resources.tasks import small_task
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile


def run(command: List[str], check: bool = True, capture_output: bool = False):
    cmd = ["conda", "run", "--name", "example", "/bin/bash", "-c", " ".join(command)]

    return subprocess.run(cmd, check=check, capture_output=capture_output)


# change the name of this function to something more descriptive
@small_task
def conda_task(input_file: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    # You can run conda packages as a subprocess:

    run(
        [
            "put",
            "conda",
            "command",
            "here",
        ]
    )
    ...
    output_location = f"{output_directory.remote_directory}/YOUR_OUTPUT_FILE_NAME"
    return LatchFile(str("/root/LOCAL_FILE_IN_TASK"), output_location)
