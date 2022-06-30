from textwrap import dedent
from typing import Callable

from flytekit import workflow as _workflow

from latch.types.metadata import LatchMetadata


def workflow(metadata: LatchMetadata):
    def decorator(f: Callable):
        f.__doc__ = f"{dedent(f.__doc__)}\n\n" + str(metadata)
        return _workflow(f)

    return decorator
