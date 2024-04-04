from dataclasses import fields, is_dataclass, make_dataclass
from typing import Any, Dict, List, Optional, Type, Union, get_args, get_origin

from flytekit.core.annotation import FlyteAnnotation
from typing_extensions import Annotated, TypeAlias, TypeGuard

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.utils import identifier_from_str

from ..utils import is_primitive_type, is_primitive_value, type_repr

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

        if len(set(parsed_types)) != 1:
            raise ValueError(
                "Generic Lists are not supported - please"
                f" ensure that all elements in {name} are of the same type",
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


def is_list_type(typ: Type) -> TypeGuard[Type[List]]:
    return get_origin(typ) is list


def dataclass_repr(typ: Type, *, make_optionals: bool = False) -> str:
    assert is_dataclass(typ)

    lines = ["@dataclass", f"class {typ.__name__}:"]
    for f in fields(typ):
        t = f.type
        if make_optionals:
            t = Optional[t]

        lines.append(f"    {f.name}: {type_repr(t)}")

    return "\n".join(lines) + "\n\n\n"


def get_preamble(typ: Type, *, make_optionals: bool = False) -> str:
    if get_origin(typ) is Annotated:
        args = get_args(typ)
        assert len(args) > 0
        return get_preamble(args[0], make_optionals=make_optionals)

    if is_primitive_type(typ) or typ in {LatchFile, LatchDir}:
        return ""

    if get_origin(typ) in {Union, list}:
        return "".join(
            [get_preamble(t, make_optionals=make_optionals) for t in get_args(typ)]
        )

    assert is_dataclass(typ), typ

    preambles = []
    for f in fields(typ):
        t = f.type
        if make_optionals:
            t = Optional[f.type]

        preambles.append(get_preamble(t, make_optionals=make_optionals))

    preamble = "".join(preambles)

    return "".join([preamble, dataclass_repr(typ, make_optionals=make_optionals)])
