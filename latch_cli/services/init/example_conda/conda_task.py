import subprocess

from latch import small_task
from latch.types import LatchFile, LatchOutputDir


# change the name of this function to something more descriptive
@small_task
def conda_task(input_file: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    # You can run conda packages as a subprocess:

    subprocess.run(
        "".join(
            [
                "put",
                "conda",
                "command",
                "here",
            ]
        ),
        check=True,
        shell=True,
    )
    ...
    output_location = f"{output_directory.remote_directory}/YOUR_OUTPUT_FILE_NAME"
    return LatchFile(str("/root/LOCAL_FILE_IN_TASK"), output_location)
