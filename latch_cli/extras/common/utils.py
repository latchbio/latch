import textwrap
from typing import Type, Union, get_args, get_origin

from typing_extensions import Annotated, TypeGuard

from latch.types.directory import LatchDir
from latch.types.file import LatchFile


# todo(maximsmol): use a stateful writer that keeps track of indent level
def reindent(x: str, level: int) -> str:
    if x[0] == "\n":
        x = x[1:]
    return textwrap.indent(textwrap.dedent(x), "    " * level)


def is_primitive_type(
    typ: Type,
) -> TypeGuard[Union[Type[None], Type[str], Type[bool], Type[int], Type[float]]]:
    return typ in {Type[None], str, bool, int, float}


def is_primitive_value(val: object) -> TypeGuard[Union[None, str, bool, int, float]]:
    return is_primitive_type(type(val))


def is_blob_type(typ: Type) -> TypeGuard[Union[Type[LatchFile], Type[LatchDir]]]:
    origin = get_origin(typ)
    if origin is Annotated:
        return any([is_blob_type(sub_typ) for sub_typ in get_args(typ)])
    if origin is not None:
        return all([is_blob_type(sub_typ) for sub_typ in get_args(typ)])

    return typ in {LatchFile, LatchDir}


def type_repr(t: Type, *, add_namespace: bool = False) -> str:
    if getattr(t, "__name__", None) == "NoneType":
        return "None"

    if is_primitive_type(t) or t is LatchFile or t is LatchDir:
        return t.__name__

    if get_origin(t) is None:
        return f"{'latch_metadata.' if add_namespace else ''}{t.__name__}"

    if get_origin(t) is list:
        args = get_args(t)
        if len(args) > 0:
            return f"typing.List[{type_repr(args[0], add_namespace=add_namespace)}]"

        return "typing.List"

    if get_origin(t) is Union:
        args = get_args(t)

        assert len(args) > 0

        return (
            f"typing.Union[{', '.join([type_repr(arg, add_namespace=add_namespace) for arg in args])}]"
        )

    return getattr(t, "__name__", repr(t))
