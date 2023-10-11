import json
import textwrap as tw
from dataclasses import fields, is_dataclass, make_dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, get_args, get_origin

import yaml
from typing_extensions import TypeAlias, TypeGuard

from latch_cli.snakemake.workflow import reindent
from latch_cli.utils import identifier_suffix_from_str

JSONValue: TypeAlias = Union[int, str, bool, float, List["JSONValue"], "JSONDict"]
JSONDict: TypeAlias = Dict[str, "JSONValue"]


def parse_type(v: JSONValue, name: Optional[str] = None) -> Type:
    if is_primitive_value(v):
        return type(v)

    if isinstance(v, list):
        parsed_types = tuple(parse_type(x) for x in v)
        return List[Union[parsed_types]]

    if name is None:
        name = "SnakemakeRecord"

    fields = {}
    for k, x in v.items():
        fields[identifier_suffix_from_str(k)] = parse_type(
            x, identifier_suffix_from_str(k)
        )

    return make_dataclass(name, fields.items())


def parse_config(config_path: Path) -> Dict[str, Type]:
    res: JSONValue = yaml.safe_load(config_path.read_text())

    if not isinstance(res, dict):
        return {}

    params: Dict[str, Type] = {}
    for k, v in res.items():
        params[k] = parse_type(v, identifier_suffix_from_str(k))

    return params


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


def type_repr(t: Type) -> str:
    if is_dataclass(t) or is_primitive_type(t):
        return t.__name__

    return repr(t)


def dataclass_repr(typ: Type) -> str:
    assert is_dataclass(typ)

    lines = ["@dataclass", f"class {typ.__name__}:"]
    for f in fields(typ):
        lines.append(f"    {f.name}: {type_repr(f.type)}")

    return "\n".join(lines) + "\n\n\n"


def get_preamble(typ: Type) -> str:
    if typ in {str, int, bool}:
        return ""

    if get_origin(typ) in {Union, list}:
        return "".join([get_preamble(t) for t in get_args(typ)])

    assert is_dataclass(typ), typ

    preamble = "".join([get_preamble(f.type) for f in fields(typ)])

    return "".join([preamble, dataclass_repr(typ)])


def generate_metadata(config_path: Path):
    parsed = parse_config(config_path)

    preambles = []
    params = []

    for k, typ in parsed.items():
        preambles.append(get_preamble(typ))
        params.append(
            reindent(
                f"""\
                {repr(k)}: SnakemakeParameter(
                    display_name={repr(k)},
                    type={type_repr(typ)},
                ),""",
                2,
            )
        )

    with open("latch_metadata.py", "w") as f:
        f.write(
            reindent(
                f"""
                from dataclasses import dataclass
                import typing

                from latch.types.metadata import SnakemakeParameter, SnakemakeMetadata, LatchAuthor
                from latch.types.directory import LatchDir
                from latch.types.file import LatchFile

                __preambles__
                SnakemakeMetadata(
                    output_dir=LatchDir("latch:///your_output_directory``"),
                    display_name="Your Workflow Name",
                    author=LatchAuthor(
                        name="Your Name",
                    ),
                    parameters={{
                __params__
                    }},
                )
                """,
                0,
            )
            .replace("__preambles__", "".join(preambles))
            .replace("__params__", "\n".join(params))
        )


if __name__ == "__main__":
    generate_metadata(
        Path("/Users/ayush/Desktop/core/latch/scratch/snakemake/config.yaml")
    )
