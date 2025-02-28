from __future__ import annotations

import sys
from dataclasses import fields, is_dataclass, make_dataclass
from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Type, Union, get_args, get_origin

from flytekit.core.annotation import FlyteAnnotation
from typing_extensions import TypeAlias, TypeGuard

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.utils import identifier_from_str

JSONValue: TypeAlias = Union[int, str, bool, float, None, List["JSONValue"], "JSONDict"]
JSONDict: TypeAlias = Dict[str, "JSONValue"]

if sys.version_info >= (3, 10):
    from types import UnionType
else:
    UnionType = Union

# ayush: yoinked from console
valid_extensions = {
    "bed",
    "vcf",
    "css",
    "csv",
    "gif",
    "png",
    "pdf",
    "webp",
    "xhtml",
    "xlsx",
    "xml",
    "py",
    "log",
    "json",
    "gz",
    "mmtf",
    "deseqreport",
    "sam",
    "bam",
    "cram",
    "tsv",
    "tab",
    "sf",
    "txt",
    "text",
    "license",
    "readme",
    "r",
    "rscript",
    "md",
    "markdown",
    "markdn",
    "mdown",
    "htm",
    "html",
    "ipynb",
    "jpeg",
    "jpg",
    "jif",
    "jpe",
    "jfif",
    "js",
    "mjs",
    "es",
    "ts",
    "jsx",
    "tsx",
    "svg",
    "svgz",
    "fasta",
    "fna",
    "fa",
    "ffn",
    "faa",
    "frn",
    "fastq",
    "fq",
    "pdb",
    "pdb1",
    "ent",
    "brk",
    "ml2",
    "mol2",
    "sy2",
    "hdf",
    "h4",
    "hdf4",
    "he4",
    "h5",
    "hdf5",
    "he5",
    "h5ad",
    "yaml",
    "yml",
}


def parse_type(v: JSONValue, name: Optional[str] = None, *, infer_files: bool = False) -> Type:
    if v is None:
        return str

    if infer_files and isinstance(v, str):
        if any([v.endswith(ext) for ext in valid_extensions]):
            return LatchFile
        elif v.endswith("/"):
            return LatchDir

    if is_primitive_value(v):
        return type(v)

    if isinstance(v, list):
        parsed_types = tuple(parse_type(x, name, infer_files=infer_files) for x in v)

        if len(set(parsed_types)) != 1:
            raise ValueError(
                "Generic Lists are not supported - please"
                f" ensure that all elements in {name} are of the same type"
            )
        typ = parsed_types[0]
        if typ in {LatchFile, LatchDir}:
            return Annotated[List[typ], FlyteAnnotation({"size": len(v)})]
        return List[typ]

    assert isinstance(v, dict)

    if name is None:
        name = "SnakemakeRecord"

    fields: Dict[str, Type] = {}
    for k, x in v.items():
        fields[identifier_from_str(k)] = parse_type(x, f"{name}_{k}", infer_files=infer_files)

    return make_dataclass(identifier_from_str(name), fields.items())


# returns raw value and generated default
def parse_value(t: Type, v: JSONValue):
    if v is None:
        return None, None

    if get_origin(t) is Annotated:
        args = get_args(t)
        assert len(args) > 0
        return parse_value(args[0], v)

    if t in {LatchFile, LatchDir}:
        # ayush: autogenerated defaults don't make sense for files/dirs since their
        # value in the config is their local path
        return v, None

    if is_primitive_value(v):
        return v, v

    if isinstance(v, list):
        assert get_origin(t) is list

        args = get_args(t)
        assert len(args) > 0

        sub_type = args[0]
        res = [parse_value(sub_type, x) for x in v]
        return [x[0] for x in res], [x[1] for x in res]

    assert isinstance(v, dict), v
    assert is_dataclass(t), t

    ret = {}
    defaults = {}
    fs = {identifier_from_str(f.name): f for f in fields(t)}

    for k, x in v.items():
        sanitized = identifier_from_str(k)
        assert sanitized in fs, sanitized
        val, default = parse_value(fs[sanitized].type, x)
        ret[sanitized] = val
        defaults[sanitized] = default

    return t(**ret), t(**defaults)


def is_primitive_type(
    typ: Type,
) -> TypeGuard[Union[Type[None], Type[str], Type[bool], Type[int], Type[float]]]:
    return typ in {type(None), str, bool, int, float}


def is_primitive_value(val: object) -> TypeGuard[Union[None, str, bool, int, float]]:
    return is_primitive_type(type(val))


def is_list_type(typ: Type) -> TypeGuard[Type[List]]:
    return get_origin(typ) is list


def type_repr(t: type[Any] | str, *, add_namespace: bool = False) -> str:
    if isinstance(t, str):
        return type_repr(eval(t), add_namespace=add_namespace)

    if is_primitive_type(t) or t in {LatchFile, LatchDir}:
        return t.__name__

    if get_origin(t) is None:
        return f"{'latch_metadata.' if add_namespace else ''}{t.__name__}"

    if get_origin(t) is list:
        args = get_args(t)
        if len(args) > 0:
            return f"typing.List[{type_repr(args[0], add_namespace=add_namespace)}]"

        return "typing.List"

    if get_origin(t) is dict:
        args = get_args(t)
        if len(args) != 2:
            return "typing.Dict"

        s = ", ".join([type_repr(x, add_namespace=add_namespace) for x in args])
        return f"typing.Dict[{s}]"

    if get_origin(t) is Union:
        args = get_args(t)
        if len(args) != 2 or args[1] is not type(None):
            raise ValueError("Union types other than Optional are not yet supported")

        return f"typing.Optional[{type_repr(args[0], add_namespace=add_namespace)}]"

    if get_origin(t) is Annotated:
        args = get_args(t)
        assert len(args) > 1
        if isinstance(args[1], FlyteAnnotation):
            if "output" in args[1].data:
                return "LatchOutputDir"

            return (
                f"typing_extensions.Annotated[{type_repr(args[0], add_namespace=add_namespace)},"
                f" FlyteAnnotation({repr(args[1].data)})]"
            )
        return type_repr(args[0], add_namespace=add_namespace)

    return t.__name__


def dataclass_repr(typ: type[Any]) -> str:
    assert is_dataclass(typ)

    lines = ["@dataclass", f"class {typ.__name__}:"]
    for f in fields(typ):
        lines.append(f"    {f.name}: {type_repr(f.type)}")

    return "\n".join(lines) + "\n\n\n"


def enum_repr(typ: Type) -> str:
    assert issubclass(typ, Enum), typ

    lines = [f"class {typ.__name__}(Enum):"]
    for name, val in typ._member_map_.items():
        lines.append(f"    {name} = {repr(val.value)}")

    return "\n".join(lines) + "\n\n\n"


def get_preamble(typ: type[Any] | str, *, defined_names: set[str] | None = None) -> str:
    # ayush: some dataclass fields have strings as their types so attempt to eval them here
    if isinstance(typ, str):
        try:
            typ = eval(typ)
        except Exception:
            return ""

    assert not isinstance(typ, str)

    if defined_names is None:
        defined_names = set()

    if get_origin(typ) is Annotated:
        args = get_args(typ)
        assert len(args) > 0
        return get_preamble(args[0], defined_names=defined_names)

    if is_primitive_type(typ) or typ in {LatchFile, LatchDir}:
        return ""

    if get_origin(typ) in {Union, UnionType, list, dict}:
        return "".join([get_preamble(t, defined_names=defined_names) for t in get_args(typ)])

    if typ.__name__ in defined_names:
        return ""

    defined_names.add(typ.__name__)

    if issubclass(typ, Enum):
        return enum_repr(typ)

    assert is_dataclass(typ), typ

    preamble = "".join([get_preamble(f.type, defined_names=defined_names) for f in fields(typ)])

    return "".join([preamble, dataclass_repr(typ)])


def validate_snakemake_type(name: str, t: Type, param: Any) -> None:
    if t is type(None):
        return param is None

    elif is_primitive_type(t) or t in {LatchFile, LatchDir}:
        if param is None:
            raise ValueError(
                f"Parameter {name} of type {t} cannot be None. Either specify a"
                " non-None default value or use the Optional type"
            )
        if not isinstance(param, t):
            raise ValueError(f"Parameter {name} must be of type {t}, not {type(param)}")

    elif get_origin(t) is Union:
        args = get_args(t)
        # only Optional types supported
        if len(args) != 2 or args[1] is not type(None):
            raise ValueError(
                f"Failed to parse input param {param}. Union types other than"
                " Optional are not yet supported in Snakemake workflows."
            )
        if param is None:
            return
        validate_snakemake_type(name, args[0], param)

    elif get_origin(t) is Annotated:
        args = get_args(t)
        assert len(args) > 0
        validate_snakemake_type(name, args[0], param)

    elif is_list_type(t):
        args = get_args(t)
        if len(args) == 0:
            raise ValueError(
                "Generic Lists are not supported - please specify a subtype, e.g. List[LatchFile]"
            )
        list_typ = args[0]
        for i, val in enumerate(param):
            validate_snakemake_type(f"{name}[{i}]", list_typ, val)

    else:
        assert is_dataclass(t)
        for field in fields(t):
            validate_snakemake_type(f"{name}.{field.name}", field.type, getattr(param, field.name))
        for i, val in enumerate(param):
            validate_snakemake_type(f"{name}[{i}]", list_typ, val)
