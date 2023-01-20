import inspect
from textwrap import dedent
from typing import Callable, Union

from flytekit import workflow as _workflow
from flytekit.core.workflow import PythonFunctionWorkflow

from latch.types.metadata import LatchMetadata


# this weird Union thing is to ensure backwards compatibility,
# so that when users call @workflow without any arguments or
# parentheses, the workflow still serializes as expected
def workflow(
    metadata: Union[LatchMetadata, Callable]
) -> Union[PythonFunctionWorkflow, Callable]:
    if isinstance(metadata, Callable):
        return _workflow(metadata)
    else:

        def decorator(f: Callable):
            if f.__doc__ is None:
                f.__doc__ = f"{f.__name__}\n\nSample Description"
            short_desc, long_desc = f.__doc__.split("\n", 1)

            signature = inspect.signature(f)
            wf_params = signature.parameters

            in_meta_not_in_wf = []
            not_in_meta_in_wf = []

            for param in metadata.parameters:
                if param not in wf_params:
                    in_meta_not_in_wf.append(param)

            for param in wf_params:
                if param not in metadata.parameters:
                    not_in_meta_in_wf.append(param)

            if len(in_meta_not_in_wf) > 0 or len(not_in_meta_in_wf) > 0:
                error_str = (
                    "Inconsistency detected between parameters in your `LatchMetadata`"
                    " object and parameters in your workflow signature.\n\n"
                )

                if len(in_meta_not_in_wf) > 0:
                    error_str += (
                        "The following parameters appear in your `LatchMetadata` object"
                        " but not in your workflow signature:\n\n"
                    )
                    for param in in_meta_not_in_wf:
                        error_str += f"    \x1b[1m{param}\x1b[22m\n"
                    error_str += "\n"

                if len(not_in_meta_in_wf) > 0:
                    error_str += (
                        "The following parameters appear in your workflow signature but"
                        " not in your `LatchMetadata` object:\n\n"
                    )
                    for param in not_in_meta_in_wf:
                        error_str += f"    \x1b[1m{param}\x1b[22m\n"
                    error_str += "\n"

                error_str += (
                    "Please resolve these inconsistencies and ensure that your"
                    " `LatchMetadata` object and workflow signature have the same"
                    " parameters."
                )

                raise ValueError(error_str)

            f.__doc__ = f"{short_desc}\n{dedent(long_desc)}\n\n" + str(metadata)
            return _workflow(f)

        return decorator
