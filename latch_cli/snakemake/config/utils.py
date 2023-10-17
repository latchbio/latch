from dataclasses import asdict, fields, is_dataclass, make_dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Type, Union, get_args, get_origin

from typing_extensions import TypeAlias, TypeGuard, TypeVar

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.utils import identifier_suffix_from_str

JSONValue: TypeAlias = Union[int, str, bool, float, None, List["JSONValue"], "JSONDict"]
JSONDict: TypeAlias = Dict[str, "JSONValue"]


def parse_type(v: JSONValue, name: Optional[str] = None) -> Type:
    if v is None:
        return str

    if is_primitive_value(v):
        return type(v)

    if isinstance(v, list):
        parsed_types = tuple(parse_type(x) for x in v)
        return List[Union[parsed_types]]

    assert isinstance(v, dict)

    if name is None:
        name = "SnakemakeRecord"

    fields = {}
    for k, x in v.items():
        fields[identifier_suffix_from_str(k)] = parse_type(
            x, identifier_suffix_from_str(k)
        )

    return make_dataclass(name, fields.items())


def is_primitive_type(
    typ: Type,
) -> TypeGuard[Union[Type[str], Type[bool], Type[int], Type[float]]]:
    return _is_primitive(t=typ)


def is_primitive_value(
    val: Any,
) -> TypeGuard[Union[str, bool, int, float]]:
    return _is_primitive(v=val)


def _is_primitive(
    *,
    t: Optional[Type] = None,
    v: Optional[Any] = None,
) -> bool:
    if v is not None:
        t = type(v)

    assert t is not None

    return t in {str, bool, int, float}


def type_repr(t: Type, *, add_namespace: bool = False) -> str:
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

    return t.__name__


def dataclass_repr(typ: Type) -> str:
    assert is_dataclass(typ)

    lines = ["@dataclass", f"class {typ.__name__}:"]
    for f in fields(typ):
        lines.append(f"    {f.name}: {type_repr(f.type)}")

    return "\n".join(lines) + "\n\n\n"


def get_preamble(typ: Type) -> str:
    if is_primitive_type(typ):
        return ""

    if get_origin(typ) in {Union, list}:
        return "".join([get_preamble(t) for t in get_args(typ)])

    assert is_dataclass(typ), typ

    preamble = "".join([get_preamble(f.type) for f in fields(typ)])

    return "".join([preamble, dataclass_repr(typ)])
