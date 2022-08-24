from textwrap import dedent
from typing import Callable, Union

from flytekit import workflow as _workflow

from latch.types.metadata import LatchMetadata


# this weird Union thing is to ensure backwards compatibility,
# so that when users call @workflow without any arguments or
# parentheses, the workflow still serializes as expected
def workflow(metadata: Union[LatchMetadata, Callable]):
    if isinstance(metadata, Callable):
        return _workflow(metadata)
    else:

        def decorator(f: Callable):
            if f.__doc__ is None:
                f.__doc__ = f"{f.__name__}\n\nSample Description"
            short_desc, long_desc = f.__doc__.split("\n", 1)
            f.__doc__ = f"{short_desc}\n{dedent(long_desc)}\n\n" + str(metadata)
            return _workflow(f)

        return decorator
