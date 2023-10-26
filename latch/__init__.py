"""The Latch SDK

A commandline toolchain to define and register serverless workflows with the
Latch platform.
"""

from latch._deprecation import _deprecated, _deprecated_import
from latch.functions.messages import message
from latch.functions.operators import (
    combine,
    group_tuple,
    inner_join,
    latch_filter,
    left_join,
    outer_join,
    right_join,
)
from latch.resources.conditional import create_conditional_section
from latch.resources.map_tasks import map_task
from latch.resources.reference_workflow import workflow_reference
from latch.resources.tasks import (
    custom_task,
    custom_memory_optimized_task,
    large_gpu_task,
    large_task,
    medium_task,
    small_gpu_task,
    small_task,
)
from latch.resources.workflow import workflow

message = _deprecated_import("message", "latch.functions.messages")(message)

combine = _deprecated()(combine)
group_tuple = _deprecated()(group_tuple)
inner_join = _deprecated()(inner_join)
latch_filter = _deprecated()(latch_filter)
left_join = _deprecated()(left_join)
outer_join = _deprecated()(outer_join)
right_join = _deprecated()(right_join)

create_conditional_section = _deprecated_import(
    "create_conditional_section", "latch.resources.conditional"
)(create_conditional_section)

map_task = _deprecated_import("map_task", "latch.resources.map_tasks")(map_task)

workflow_reference = _deprecated_import(
    "workflow_reference", "latch.resources.reference_workflow"
)(workflow_reference)

custom_task = _deprecated_import("custom_task", "latch.resources.tasks")(custom_task)
large_gpu_task = _deprecated_import("large_gpu_task", "latch.resources.tasks")(
    large_gpu_task
)
large_task = _deprecated_import("large_task", "latch.resources.tasks")(large_task)
medium_task = _deprecated_import("medium_task", "latch.resources.tasks")(medium_task)
small_gpu_task = _deprecated_import("small_gpu_task", "latch.resources.tasks")(
    small_gpu_task
)
small_task = _deprecated_import("small_task", "latch.resources.tasks")(small_task)

workflow = _deprecated_import("workflow", "latch.resources.workflow")(workflow)
