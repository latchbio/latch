from latch import small_task
from latch.types import LatchFile, LatchOutputDir


@small_task
def task(input_file: LatchFile, output_directory: LatchOutputDir) -> LatchFile:
    raise NotImplementedError("Task not implemented.")
