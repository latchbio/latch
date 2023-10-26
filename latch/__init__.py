"""The Latch SDK

A commandline toolchain to define and register serverless workflows with the
Latch platform.
"""

import warnings
from typing import Any, Dict

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
    custom_memory_optimized_task,
    custom_task,
    large_gpu_task,
    large_task,
    medium_task,
    small_gpu_task,
    small_task,
)
from latch.resources.workflow import workflow

_deprecation_version = "3.0.0"


def _warn(name: str, import_path: str):
    warnings.warn(
        f"Importing `{name}` directly from `latch` is deprecated, and will"
        f" be removed in version {_deprecation_version}.\n\n    Please use"
        f" the full import `from {import_path} import {name}`\n",
        DeprecationWarning,
    )


def message(typ: str, data: Dict[str, Any]):
    from latch.functions.messages import message as _message

    _warn("message", "latch.functions.messages")

    return _message(typ, data)
