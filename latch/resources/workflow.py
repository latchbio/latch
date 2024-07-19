import inspect
import sys
import typing
from dataclasses import is_dataclass
from textwrap import dedent
from typing import Any, Callable, Dict, Union, get_args, get_origin
from typing_extensions import TypeAlias
from typing_extensions import TypeGuard

import click
import os
from flytekit import workflow as _workflow
from flytekit.core.workflow import PythonFunctionWorkflow

from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter
from latch_cli.utils import best_effort_display_name


if sys.version_info >= (3, 10):
    from types import UnionType
else:
    # NB: `types.UnionType`, available since Python 3.10, is **not** a `type`, but is a class.
    # We declare an empty class here to use in the instance checks below.
    class UnionType:
        pass


# NB: since `_GenericAlias` is a private attribute of the `typing` module, mypy doesn't find it
TypeAnnotation: TypeAlias = Union[type, typing._GenericAlias, UnionType]  # type: ignore[name-defined]
"""
A function parameter's type annotation may be any of the following:
    1) `type`, when declaring any of the built-in Python types
    2) `typing._GenericAlias`, when declaring generic collection types or union types using pre-PEP
        585 and pre-PEP 604 syntax (e.g. `List[int]`, `Optional[int]`, or `Union[int, None]`)
    3) `types.UnionType`, when declaring union types using PEP604 syntax (e.g. `int | None`)
    4) `types.GenericAlias`, when declaring generic collection types using PEP 585 syntax (e.g.
       `list[int]`)

`types.GenericAlias` is a subclass of `type`, but `typing._GenericAlias` and `types.UnionType` are
not and must be considered explicitly.
"""

# TODO When dropping support for Python 3.9, deprecate this in favor of performing instance checks
# directly on the `TypeAnnotation` union type.
# NB: since `_GenericAlias` is a private attribute of the `typing` module, mypy doesn't find it
TYPE_ANNOTATION_TYPES = (type, typing._GenericAlias, UnionType)  # type: ignore[attr-defined]


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
    metadata: Union[LatchMetadata, Callable],
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

            if not _is_valid_samplesheet_parameter_type(wf_params[name].annotation):
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


def _is_valid_samplesheet_parameter_type(annotation: Any) -> TypeGuard[TypeAnnotation]:
    """Check if a workflow parameter is hinted with a valid type for a samplesheet LatchParameter.

    Currently, a samplesheet LatchParameter must be defined as a list of dataclasses, or as an
    `Optional` list of dataclasses when the parameter is part of a `ForkBranch`.

    Args:
        parameter: A parameter from the workflow function's signature.

    Returns:
        True if the parameter is annotated as a list of dataclasses, or as an `Optional` list of
        dataclasses.
        False otherwise.
    """
    # If the parameter did not have a type annotation, short-circuit and return False
    if not _is_type_annotation(annotation):
        return False

    return _is_list_of_dataclasses_type(annotation) or (
        _is_optional_type(annotation)
        and _is_list_of_dataclasses_type(_unpack_optional_type(annotation))
    )


def _is_list_of_dataclasses_type(dtype: TypeAnnotation) -> bool:
    """Check if the type is a list of dataclasses.

    Args:
        dtype: A type.

    Returns:
        True if the type is a list of dataclasses.
        False otherwise.

    Raises:
        TypeError: If the input is not a valid `TypeAnnotation` type (see above).
    """
    if not isinstance(dtype, TYPE_ANNOTATION_TYPES):
        raise TypeError(f"Expected type annotation, got {type(dtype)}: {dtype}")

    origin = get_origin(dtype)
    args = get_args(dtype)

    return (
        origin is not None
        and inspect.isclass(origin)
        and issubclass(origin, list)
        and len(args) == 1
        and is_dataclass(args[0])
    )


def _is_optional_type(dtype: TypeAnnotation) -> bool:
    """Check if a type is `Optional`.

    An optional type may be declared using three syntaxes: `Optional[T]`, `Union[T, None]`, or `T |
    None`. All of these syntaxes is supported by this function.

    Args:
        dtype: A type.

    Returns:
        True if the type is a union type with exactly two elements, one of which is `None`.
        False otherwise.

    Raises:
        TypeError: If the input is not a valid `TypeAnnotation` type (see above).
    """
    if not isinstance(dtype, TYPE_ANNOTATION_TYPES):
        raise TypeError(f"Expected type annotation, got {type(dtype)}: {dtype}")

    origin = get_origin(dtype)
    args = get_args(dtype)

    # Optional[T] has `typing.Union` as its origin, but PEP604 syntax (e.g. `int | None`) has
    # `types.UnionType` as its origin.
    return (
        origin is not None
        and (origin is Union or origin is UnionType)
        and len(args) == 2
        and type(None) in args
    )


def _unpack_optional_type(dtype: TypeAnnotation) -> type:
    """Given a type of `Optional[T]`, return `T`.

    Args:
        dtype: A type of `Optional[T]`, `T | None`, or `Union[T, None]`.

    Returns:
        The type `T`.

    Raises:
        TypeError: If the input is not a valid `TypeAnnotation` type (see above).
        ValueError: If the input type is not `Optional[T]`.
    """
    if not isinstance(dtype, TYPE_ANNOTATION_TYPES):
        raise TypeError(f"Expected type annotation, got {type(dtype)}: {dtype}")

    if not _is_optional_type(dtype):
        raise ValueError(f"Expected `Optional[T]`, got {type(dtype)}: {dtype}")

    # Types declared as `Optional[T]` or `T | None` should have the non-None type as the first
    # argument.  However, it is technically correct for someone to write `None | T`, so we shouldn't
    # make assumptions about the argument ordering. (And I'm not certain the ordering is guaranteed
    # anywhere by Python spec.)
    base_type = [arg for arg in get_args(dtype) if arg is not type(None)][0]

    return base_type


# NB: `inspect.Parameter.annotation` is typed as `Any`, so here we narrow the type.
def _is_type_annotation(annotation: Any) -> TypeGuard[TypeAnnotation]:
    """Check if the annotation on an `inspect.Parameter` instance is a type annotation.

    If the corresponding parameter **did not** have a type annotation, `annotation` is set to the
    special class variable `inspect.Parameter.empty`. Otherwise, the annotation should be a valid
    type annotation.

    Args:
        annotation: The annotation on an `inspect.Parameter` instance.

    Returns:
        True if the type annotation is not `inspect.Parameter.empty`.
        False otherwise.

    Raises:
        TypeError: If the annotation is neither a valid `TypeAnnotation` type (see above) nor
        `inspect.Parameter.empty`.
    """
    # NB: `inspect.Parameter.empty` is a subclass of `type`, so this check passes for unannotated
    # parameters.
    if not isinstance(annotation, TYPE_ANNOTATION_TYPES):
        raise TypeError(f"Annotation must be a type, not {type(annotation).__name__}")

    return annotation is not inspect.Parameter.empty
