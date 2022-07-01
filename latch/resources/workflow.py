from textwrap import dedent
from typing import Callable, Union

from flytekit import workflow as _workflow

from latch.types.metadata import LatchMetadata


def workflow(m: Union[LatchMetadata, Callable]):
    if isinstance(m, Callable):
        return _workflow(m)
    else:

        def decorator(f: Callable):
            f.__doc__ = f"{dedent(f.__doc__)}\n\n" + str(m)
            return _workflow(f)

        return decorator
