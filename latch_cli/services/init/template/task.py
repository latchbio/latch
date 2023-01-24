from latch import small_task
from latch.types import LatchFile, LatchOutputDir


@small_task
def task(input_file: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    ...

    output_location = f"{output_directory.remote_directory}/YOUR_OUTPUT_FILE_NAME"

    return LatchFile(str("/root/LOCAL_FILE_IN_TASK"), output_location)
