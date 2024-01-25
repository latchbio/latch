from dataclasses import asdict, fields, is_dataclass, make_dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Type, Union, get_args, get_origin

from typing_extensions import TypeAlias, TypeGuard, TypeVar

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.utils import identifier_from_str

JSONValue: TypeAlias = Union[int, str, bool, float, None, List["JSONValue"], "JSONDict"]
JSONDict: TypeAlias = Dict[str, "JSONValue"]

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


def parse_type(
    v: JSONValue, name: Optional[str] = None, *, infer_files: bool = False
) -> Type:
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
        parsed_types = tuple(
            parse_type(
                x,
                name,
                infer_files=infer_files,
            )
            for x in v
        )
        if len(set(parsed_types)) > 1:
            raise ValueError("all types in List must be same")
        return List[parsed_types[0]]

    assert isinstance(v, dict)

    if name is None:
        name = "SnakemakeRecord"

    fields: Dict[str, Type] = {}
    for k, x in v.items():
        fields[identifier_from_str(k)] = parse_type(
            x,
            k,
            infer_files=infer_files,
        )

    return make_dataclass(identifier_from_str(name), fields.items())


# returns raw value and generated default
def parse_value(t: Type, v: JSONValue):
    if v is None:
        return None, None

    if t in {LatchFile, LatchDir}:
        # ayush: autogenerated defaults don't make sense for files/dirs since their
        # value in the config is their local path
        return v, None

    if is_primitive_value(v):
        return v, v

    if isinstance(v, list):
        assert get_origin(t) is list

        sub_type = get_args(t)[0]
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
    return typ in {Type[None], str, bool, int, float}


def is_primitive_value(val: object) -> TypeGuard[Union[None, str, bool, int, float]]:
    return is_primitive_type(type(val))


def is_list_type(typ: Type) -> TypeGuard[Type[List]]:
    return get_origin(typ) is list


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
    if is_primitive_type(typ) or typ in {LatchFile, LatchDir}:
        return ""

    if get_origin(typ) in {Union, list}:
        return "".join([get_preamble(t) for t in get_args(typ)])

    assert is_dataclass(typ), typ

    preamble = "".join([get_preamble(f.type) for f in fields(typ)])

    return "".join([preamble, dataclass_repr(typ)])
