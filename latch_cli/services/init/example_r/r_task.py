import subprocess

from latch.resources.tasks import small_task
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile


# change the name of this function to something more descriptive
@small_task
def r_task(input_file: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    # You can run R files as a subprocess:

    subprocess.run(
        " ".join(
            [
                "Rscript",
                "path/to/your_script.R",
                "command_line_arg_1",
                "command_line_arg_2",
            ]
        ),
        check=True,
        shell=True,
    )
    ...
    output_location = f"{output_directory.remote_directory}/YOUR_OUTPUT_FILE_NAME"
    return LatchFile(str("/root/LOCAL_FILE_IN_TASK"), output_location)
