from latch.resources.tasks import small_task
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile


@small_task
def task(input_file: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    raise NotImplementedError("Task not implemented.")
