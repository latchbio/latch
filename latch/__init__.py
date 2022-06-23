"""The Latch SDK

A commandline toolchain to define and register serverless workflows with the
Latch platform.
"""

from latch.resources.conditional import create_conditional_section
from latch.resources.map_tasks import map_task
from latch.resources.tasks import (
    large_gpu_task,
    large_task,
    medium_task,
    small_gpu_task,
    small_task,
)
from latch.resources.workflow import workflow
from latch.functions.messages import message
