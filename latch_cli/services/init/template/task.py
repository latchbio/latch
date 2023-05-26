from latch.resources.tasks import small_task
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile


@small_task
def task(input_file: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    ...

    output_location = f"{output_directory.remote_directory}/YOUR_OUTPUT_FILE_NAME"

    return LatchFile(str("/root/LOCAL_FILE_IN_TASK"), output_location)
