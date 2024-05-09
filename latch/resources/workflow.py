import inspect
from dataclasses import is_dataclass
from textwrap import dedent
from typing import Callable, Dict, Union, get_args, get_origin

import click
import os
from flytekit import workflow as _workflow
from flytekit.core.workflow import PythonFunctionWorkflow

from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter
from latch_cli.utils import best_effort_display_name


def _generate_metadata(f: Callable) -> LatchMetadata:
    signature = inspect.signature(f)
    metadata = LatchMetadata(f.__name__, LatchAuthor())
    metadata.parameters = {
        param: LatchParameter(display_name=best_effort_display_name(param))
        for param in signature.parameters
    }
    return metadata


def _inject_metadata(f: Callable, metadata: LatchMetadata) -> None:
    if f.__doc__ is None:
        f.__doc__ = f"{f.__name__}\n\nSample Description"
    short_desc, long_desc = f.__doc__.split("\n", 1)
    f.__doc__ = f"{short_desc}\n{dedent(long_desc)}\n\n" + str(metadata)


# this weird Union thing is to ensure backwards compatibility,
# so that when users call @workflow without any arguments or
# parentheses, the workflow still serializes as expected
def workflow(
    metadata: Union[LatchMetadata, Callable]
) -> Union[PythonFunctionWorkflow, Callable]:
    if isinstance(metadata, Callable):
        f = metadata
        if f.__doc__ is None or "__metadata__:" not in f.__doc__:
            metadata = _generate_metadata(f)
            _inject_metadata(f, metadata)
        return _workflow(f)

    def decorator(f: Callable):
        signature = inspect.signature(f)
        wf_params = signature.parameters

        updated_params: Dict[str, LatchParameter] = {}
        for wf_param in wf_params:
            updated_params[wf_param] = (
                LatchParameter(display_name=best_effort_display_name(wf_param))
                if wf_param not in metadata.parameters
                else metadata.parameters[wf_param]
            )
        metadata.parameters = updated_params

        in_meta_not_in_wf = []
        for meta_param in metadata.parameters:
            if meta_param not in wf_params:
                in_meta_not_in_wf.append(meta_param)

        if len(in_meta_not_in_wf) > 0:
            error_str = (
                "Inconsistency detected between parameters in your `LatchMetadata`"
                " object and parameters in your workflow signature.\n\n"
                "The following parameters appear in your `LatchMetadata` object"
                " but not in your workflow signature:\n\n"
            )
            for meta_param in in_meta_not_in_wf:
                error_str += f"    \x1b[1m{meta_param}\x1b[22m\n"
            error_str += (
                "\nPlease resolve these inconsistencies and ensure that your"
                " `LatchMetadata` object and workflow signature have the same"
                " parameters."
            )
            click.secho(error_str, fg="red")
            raise click.exceptions.Exit(1)

        for name, meta_param in metadata.parameters.items():
            if meta_param.samplesheet is not True:
                continue

            annotation = wf_params[name].annotation

            origin = get_origin(annotation)
            args = get_args(annotation)
            valid = (
                origin is not None
                and issubclass(origin, list)
                and is_dataclass(args[0])
            )
            if not valid:
                click.secho(
                    f"parameter marked as samplesheet is not valid: {name} "
                    f"in workflow {f.__name__} must be a list of dataclasses",
                    fg="red",
                )
                raise click.exceptions.Exit(1)

        _inject_metadata(f, metadata)

        # note(aidan): used for only serialize_in_container
        wf_name_override = os.environ.get("LATCH_WF_NAME_OVERRIDE")
        if wf_name_override is not None and wf_name_override.strip() == "":
            wf_name_override = None

        return _workflow(f, wf_name_override=wf_name_override)

    return decorator
