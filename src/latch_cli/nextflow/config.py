import json
import re
from dataclasses import MISSING, Field, field, make_dataclass
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Annotated, Optional, TypeVar, Union

import click
import google.protobuf.json_format as gpjson
from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import TypeEngine

from latch.ldata.path import LPath
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.types.samplesheet_item import SamplesheetItem
from latch_cli.snakemake.config.utils import get_preamble
from latch_cli.utils import best_effort_display_name, identifier_from_str

from .parse_schema import NfType, parse_schema

underscores = re.compile(r"_+")
spaces = re.compile(r"\s+")


def best_effort_title_case(s: str) -> str:
    return identifier_from_str(spaces.sub("", underscores.sub(" ", s).title()))


T = TypeVar("T")


def as_python_val(typ: type[T], value: object) -> T:
    try:
        return typ(value)
    except:
        return value


def get_field_type(
    ctx: FlyteContext, idx: int, name: str, props: NfType
) -> tuple[str, type, Field[object]]:
    if not name.isidentifier():
        name = f"param_{idx}"

    inner = get_python_type_inner(name, props)
    python_typ = inner if props["metadata"]["required"] else Optional[inner]

    # todo(ayush): defaults don't really work if the dataclass is nested but that isn't
    # supported by the nf-schema spec (however, that has never stopped nf-core developers)
    args: dict[str, object] = {}
    default_value: Optional[dict[str, object]] = None
    if "default" in props:
        python_val = as_python_val(inner, props["default"])
        # note(ayush): not dealing with lists or dicts here as those are not supported by the nf-schema spec
        if isinstance(python_val, (LatchDir, LatchFile, LPath)):
            args["default_factory"] = lambda: python_val
        else:
            args["default"] = python_val

        default_value = gpjson.MessageToDict(
            TypeEngine.to_literal(
                ctx, python_val, python_typ, TypeEngine.to_literal_type(python_typ)
            ).to_flyte_idl()
        )

    annotated = Annotated[
        python_typ,
        FlyteAnnotation({
            "display_name": best_effort_display_name(name),
            "default": default_value,
            "samplesheet": props["type"] == "samplesheet",
            "output": name == "outdir",
            **props["metadata"],
        }),
    ]

    return name, annotated, field(**args)


def get_python_type_inner(
    param_name: str, typ: NfType
) -> Union[type, Annotated[object, FlyteAnnotation]]:
    if typ["type"] == "string":
        return str
    if typ["type"] == "integer":
        return int
    if typ["type"] == "number":
        return float
    if typ["type"] == "boolean":
        return bool
    if typ["type"] == "blob":
        if typ["node_type"] == "dir":
            return LatchDir
        if typ["node_type"] == "file":
            return LatchFile

        return LPath

    if typ["type"] == "array":
        sub_type = get_python_type(param_name, typ["items"])
        return list[sub_type]

    if typ["type"] == "object":
        ctx = FlyteContextManager.current_context()
        assert ctx is not None

        no_defaults: list[tuple[str, type, Field[object]]] = []
        defaults: list[tuple[str, type, Field[object]]] = []

        for idx, (f, props) in enumerate(typ["properties"].items()):
            field_name, field_type, field_obj = get_field_type(ctx, idx, f, props)

            if field_obj.default is MISSING and field_obj.default_factory is MISSING:
                no_defaults.append((field_name, field_type, field_obj))
            else:
                defaults.append((field_name, field_type, field_obj))

        return make_dataclass(
            f"{best_effort_title_case(param_name)}Type", no_defaults + defaults
        )

    if typ["type"] == "samplesheet":
        dc = get_python_type(
            param_name, {**typ, "type": "object", "properties": typ["schema"]}
        )
        return list[SamplesheetItem[dc]]

    assert typ["type"] == "enum", f"unsupported type {typ['typ']!r}"

    variants: dict[str, str] = {}
    is_numeric = typ["flavor"] == "number" or typ["flavor"] == "integer"
    for value in typ["values"]:
        field_name = identifier_from_str(str(value))
        if is_numeric:
            field_name = identifier_from_str(f"_{value}")

        # todo(ayush): this is not strictly correct because float string representations can vary across
        # languages - however since nextflow accepts them as strings anyway i think it should be fine
        # in this specific case
        # see randomascii.wordpress.com/2013/02/07/float-precision-revisited-nine-digit-float-portability for some related stuff
        variants[field_name] = str(value)

    return Enum(f"{best_effort_title_case(param_name)}Type", variants)


def get_python_type(
    param_name: str, typ: NfType
) -> Union[type, Annotated[object, FlyteAnnotation]]:
    inner = get_python_type_inner(param_name, typ)

    if typ["metadata"]["required"]:
        return inner

    return Optional[inner]


def generate_flow(
    raw_schema_content: dict[str, object], parsed: dict[str, NfType]
) -> str:
    if "$defs" not in raw_schema_content:
        return "generated_flow = None"

    flow_elements: list[str] = []

    spec: dict[str, object]
    for key, spec in raw_schema_content["$defs"].items():
        title: str = str(spec.get("title", best_effort_display_name(key)))
        description: Optional[str] = spec.get("description")

        visible: list[str] = []
        hidden: list[str] = []

        params: Optional[dict[str, object]] = spec.get("properties")
        if params is None:
            continue

        for param_name in params:
            typ = parsed.get(param_name)
            if typ is None:
                continue

            if typ["metadata"]["required"]:
                visible.append(param_name)
                continue

            hidden.append(param_name)

        if len(visible) + len(hidden) == 0:
            continue

        if len(visible) == 0:
            if description is not None:
                flow_elements.append(
                    f"Spoiler({title!r}, Text({description!r}), Params({', '.join(repr(h) for h in hidden)}))"
                )
            else:
                flow_elements.append(
                    f"Spoiler({title!r}, Params({', '.join(repr(h) for h in hidden)}))"
                )
            continue

        section_elements = [repr(title)]

        if description is not None:
            section_elements.append(f"Text({description!r})")

        if len(visible) > 0:
            section_elements.append(f"Params({', '.join(repr(v) for v in visible)})")

        if len(hidden) > 0:
            section_elements.append(
                f"Spoiler('Optional Parameters', Params({', '.join(repr(h) for h in hidden)}))"
            )

        flow_elements.append(f"Section({', '.join(section_elements)})")

    return f"generated_flow = [{', '.join(flow_elements)}]"


def generate_metadata(
    schema_path: Path, metadata_root: Path, *, skip_confirmation: bool = False
):
    raw_schema_content: dict[str, object] = json.loads(schema_path.read_text())

    display_name: Optional[str] = raw_schema_content.get("title")
    # note(rahul): seems like it is convention to use "<pipeline-name> pipeline parameters" for schema title
    if display_name is not None and isinstance(display_name, str):
        display_name = display_name.removesuffix(" pipeline parameters")

    if metadata_root.is_file():
        if not click.confirm(
            f"A file already exists at `{metadata_root}` and must be deleted. Would you"
            " like to proceed?"
        ):
            return

        metadata_root.unlink()

    metadata_root.mkdir(exist_ok=True)

    metadata_path = metadata_root / "__init__.py"
    if not metadata_path.exists():
        metadata_path.write_text(
            dedent(f"""
            from dataclasses import dataclass

            from latch.types.metadata import (
                LatchAuthor,
                NextflowMetadata,
                NextflowParameter,
                NextflowRuntimeResources
            )
            from latch.types.directory import LatchDir

            from .generated import NextflowSchemaArgsType, generated_flow


            @dataclass
            class WorkflowArgsType(NextflowSchemaArgsType):
                # add any custom parameters here
                ...


            NextflowMetadata(
                display_name={display_name!r},
                author=LatchAuthor(
                    name="Your Name",
                ),
                parameters={{
                    "args": NextflowParameter(type=WorkflowArgsType)
                }},
                runtime_resources=NextflowRuntimeResources(
                    cpus=4,
                    memory=8,
                    storage_gib=100,
                ),
                log_dir=LatchDir("latch:///your_log_dir"),
                flow=generated_flow,
            )
        """)
        )
        click.secho(f"Generated {metadata_path}.", fg="green")

    schema = parse_schema(schema_path, strict=False)
    nf_schema_args_type = get_python_type_inner(
        "nextflow_schema_args", {"type": "object", "properties": schema}
    )

    params_path = metadata_root / "generated.py"
    if (
        params_path.exists()
        and not skip_confirmation
        and not click.confirm(
            f"File `{params_path}` already exists. Changes will be made to it. Is this ok?"
        )
    ):
        return

    params_path.write_text(
        dedent("""\
            # This file is auto-generated, PLEASE DO NOT EDIT DIRECTLY! To update, run
            #
            #   $ latch generate-metadata --nextflow nextflow_schema.json
            #
            # Add any custom logic or parameters in `latch_metadata/__init__.py`.

            import typing
            from dataclasses import dataclass, field
            from enum import Enum

            import typing_extensions
            from flytekit.core.annotation import FlyteAnnotation

            from latch.ldata.path import LPath
            from latch.types.directory import LatchDir
            from latch.types.file import LatchFile
            from latch.types.metadata import Params, Section, Spoiler, Text
            from latch.types.samplesheet_item import SamplesheetItem



            __preamble__


            __flow__
            """)
        .replace("__preamble__", get_preamble(nf_schema_args_type))
        .replace("__flow__", generate_flow(raw_schema_content, schema))
    )
    click.secho(f"Generated `{params_path}`.", fg="green")
