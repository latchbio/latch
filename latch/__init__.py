"""The Latch SDK

A commandline toolchain to define and register serverless workflows with the
Latch platform.
"""

import importlib
import sys
import warnings
from textwrap import dedent
from types import ModuleType
from typing import Callable

_imports = {
    "latch.functions.operators": [
        "combine",
        "group_tuple",
        "inner_join",
        "latch_filter",
        "left_join",
        "outer_join",
        "right_join",
    ],
    "latch.resources.conditional": ["create_conditional_section"],
    "latch.resources.map_tasks": ["map_task"],
    "latch.resources.reference_workflow": ["workflow_reference"],
    "latch.resources.tasks": [
        "custom_memory_optimized_task",
        "custom_task",
        "large_gpu_task",
        "large_task",
        "medium_task",
        "small_gpu_task",
        "small_task",
    ],
    "latch.resources.workflow": ["workflow"],
}


def deprecated(module: ModuleType, fn_name: str) -> Callable:
    fn = getattr(module, fn_name)

    def new_fn(*args, **kwargs):
        warnings.warn(
            dedent(f"""

            Importing `{fn_name}` directly from `latch` is deprecated. Please use the full import

                from {module.__name__} import {fn_name}

            This will be removed in version 3.0.0.
            """),
            DeprecationWarning,
        )

        return fn(*args, **kwargs)

    return new_fn


module = sys.modules[__name__]
for module_name, fn_names in _imports.items():
    imported = importlib.import_module(module_name)

    for fn_name in fn_names:
        setattr(module, fn_name, deprecated(imported, fn_name))


__slots__ = sum(_imports.values(), start=[])
