"""The Latch SDK

A commandline toolchain to define and register serverless workflows with the
Latch platform.
"""

from latch.resources.tasks import (large_gpu_task, large_task, small_gpu_task,
                                   small_task)
from latch.resources.workflow import workflow
