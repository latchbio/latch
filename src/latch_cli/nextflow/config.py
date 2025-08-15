import json
import re
from dataclasses import (
    MISSING,
    Field,
    dataclass,
    field,
    fields,
    is_dataclass,
    make_dataclass,
)
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
from latch_cli.services.register.utils import import_module_by_path
from latch_cli.snakemake.config.utils import dataclass_repr, get_preamble
from latch_cli.utils import best_effort_display_name, identifier_from_str

from .parse_schema import NfType, parse_schema

underscores = re.compile(r"_+")


def best_effort_title_case(s: str) -> str:
    return identifier_from_str("".join(underscores.sub(" ", s).title().split()))


T = TypeVar("T")


def as_python_type(typ: type[T], value: object) -> T:
    if issubclass(typ, (LatchFile, LatchDir)):
        if not isinstance(value, str):
            return value

        return typ(value)

    if issubclass(typ, Enum):
        try:
            return typ(value)
        except ValueError:
            return value

    return value


def get_field_type(
    ctx: FlyteContext, idx: int, name: str, props: NfType
) -> tuple[str, type, Field[object]]:
    if not name.isidentifier():
        name = f"param_{idx}"

    inner = get_python_type_inner(name, props)
    python_typ = inner if props.get("metadata", {}).get("required") else Optional[inner]

    # todo(ayush): defaults don't really work if the dataclass is nested but that isn't
    # supported by the nf-schema spec (however that has never stopped nf-core developers)
    args = {"metadata": {}}
    default_value = None
    if "default" in props:
        args["default"] = as_python_type(inner, props["default"])
        default_value = gpjson.MessageToDict(
            TypeEngine.to_literal(
                ctx, args["default"], python_typ, TypeEngine.to_literal_type(python_typ)
            ).to_flyte_idl()
        )

    annotated = Annotated[
        python_typ,
        FlyteAnnotation({
            "display_name": best_effort_display_name(name),
            "default": default_value,
            "samplesheet": props["type"] == "samplesheet",
            "output": name == "outdir",
            **props.get("metadata", {}),
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

        no_defaults: list[Union[tuple[str, type], tuple[str, type, Field[object]]]] = []
        defaults: list[Union[tuple[str, type], tuple[str, type, Field[object]]]] = []

        for idx, (f, props) in enumerate(typ["properties"].items()):
            name, annotated, field_obj = get_field_type(ctx, idx, f, props)

            append_to = no_defaults
            if field_obj.default is not MISSING:
                append_to = defaults

            append_to.append((name, annotated, field_obj))

        return make_dataclass(
            best_effort_title_case(f"{param_name} Type"), no_defaults + defaults
        )

    if typ["type"] == "samplesheet":
        dc = get_python_type(
            param_name, {**typ, "type": "object", "properties": typ["schema"]}
        )
        return list[dc]

    assert typ["type"] == "enum", f"unsupported type {typ['typ']!r}"

    variants: dict = {}
    is_numeric = typ["flavor"] == "number" or typ["flavor"] == "integer"
    for value in typ["values"]:
        name = identifier_from_str(str(value))
        if is_numeric:
            name = identifier_from_str(f"_{value}")

        variants[name] = value

    return Enum(best_effort_title_case(f"{param_name} Type"), variants)


def get_python_type(
    param_name: str, typ: NfType
) -> Union[type, Annotated[object, FlyteAnnotation]]:
    inner = get_python_type_inner(param_name, typ)

    if typ.get("metadata", {}).get("required"):
        return inner

    return Optional[inner]


dc_expr = re.compile(
    r"@dataclass\nclass WorkflowArgsType:\n(?:(    [^\n]+\n)+)", re.MULTILINE
)


def generate_metadata(
    schema_path: Path, metadata_root: Path, *, skip_confirmation: bool = False
):
    schema_content: dict = json.loads(schema_path.read_text())

    display_name: Optional[str] = schema_content.get("title")
    # rahul: seems like it is convention to use "<pipeline-name> pipeline parameters" for schema title
    suffix = " pipeline parameters"
    if (
        display_name is not None
        and isinstance(display_name, str)
        and display_name.endswith(suffix)
    ):
        display_name = display_name[: -len(suffix)]

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
            from latch.types.metadata import (
                NextflowMetadata,
                LatchAuthor,
                NextflowRuntimeResources
            )
            from latch.types.directory import LatchDir

            from .parameters import generated_parameters

            NextflowMetadata(
                display_name={display_name!r},
                author=LatchAuthor(
                    name="Your Name",
                ),
                parameters=generated_parameters,
                runtime_resources=NextflowRuntimeResources(
                    cpus=4,
                    memory=8,
                    storage_gib=100,
                ),
                log_dir=LatchDir("latch:///your_log_dir"),
            )
        """)
        )
        click.secho(f"Generated {metadata_path}.", fg="green")

    preambles = []

    schema = parse_schema(schema_path, strict=False)

    for param_name, typ in schema.items():
        py_type = get_python_type(param_name, typ)
        preambles.append(get_preamble(py_type))

    params_path = metadata_root / "parameters.py"
    if params_path.exists():
        if not skip_confirmation and not click.confirm(
            f"File `{params_path}` already exists. Changes will be made to it. Is this ok?"
        ):
            return

        params_content = params_path.read_text()

        match = dc_expr.search(params_content)

        if match is not None:
            ctx = FlyteContextManager.current_context()
            assert ctx is not None

            mod = import_module_by_path(metadata_root / "__init__.py")
            cur_args = getattr(mod, "WorkflowArgsType", None)

            assert is_dataclass(cur_args)
            existing_fields = {f.name for f in fields(cur_args)}

            new_fields = [(f.name, f.type, f) for f in fields(cur_args)]
            idx = len(existing_fields)
            for param_name, typ in schema.items():
                print(param_name)
                if param_name in existing_fields:
                    continue

                new_fields.append(get_field_type(ctx, idx, param_name, typ))
                idx += 1

            if idx == len(existing_fields):
                click.secho("No new parameters added. File unchanged.", fg="green")
                return

            new_fields.sort(key=lambda f: 0 if f[2].default is MISSING else 1)

            new_dc = make_dataclass("WorkflowArgsType", new_fields)
            params_path.write_text(dc_expr.sub(dataclass_repr(new_dc), params_content))
            click.secho(f"Successfully modified `{params_path}`.", fg="green")
            return

    params_path.write_text(
        dedent("""\
            import typing
            from dataclasses import dataclass, field
            from enum import Enum

            import typing_extensions
            from flytekit.core.annotation import FlyteAnnotation

            from latch.types.directory import LatchDir
            from latch.types.file import LatchFile
            from latch.types.metadata import NextflowParameter

            # Import these into your `__init__.py` file:
            #
            # from .parameters import generated_parameters

            __preambles__

            __dataclass__

            generated_parameters = {
                "args": NextflowParameter(type=WorkflowArgsType)
            }
            """)
        .replace("__preambles__", "".join(preambles))
        .replace(
            "__dataclass__",
            dataclass_repr(
                get_python_type_inner(
                    "Workflow Args", {"type": "object", "properties": schema}
                )
            ),
        )
    )
    click.secho(f"Generated `{params_path}`.", fg="green")
