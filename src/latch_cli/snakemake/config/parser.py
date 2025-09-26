from dataclasses import Field, field, fields, is_dataclass, make_dataclass
from pathlib import Path
from typing import Annotated, TypeVar, Union, get_args, get_origin

import click
import yaml

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.utils import Singleton
from latch_cli.snakemake.utils import reindent
from latch_cli.utils import best_effort_display_name, identifier_from_str

from ...utils import best_effort_title_case, exit
from .utils import (
    JSONValue,
    get_preamble,
    is_list_type,
    is_primitive_type,
    parse_type,
    parse_value,
    type_repr,
)

T = TypeVar("T")


class NoValue(Singleton): ...


def parse_config(config_path: Path) -> dict[str, tuple[type[T], Union[T, NoValue]]]:
    if not config_path.exists():
        raise exit(f"No config file found at {config_path}.")

    if config_path.is_dir():
        raise exit(f"Path {config_path} points to a directory.")

    try:
        res: JSONValue = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError as e:
        raise exit(
            reindent(
                f"""
                Error loading config from {config_path}:

                {e}
                """,
                0,
            )
        ) from e

    assert isinstance(res, dict)

    parsed: dict[str, tuple[type[T], T]] = {}
    for k, v in res.items():
        try:
            typ = parse_type(v, k)
        except ValueError as e:
            click.secho(f"WARNING: Skipping parameter {k}. Failed to parse type: {e}.", fg="yellow")
            continue

        default = NoValue()
        try:
            default = parse_value(typ, v)
        except AssertionError as e:
            click.secho(f"WARNING: Unable to parse default for parameter {k}: {e}.", fg="yellow")

        parsed[k] = (typ, default)

    return parsed


# todo(ayush): print informative stuff here ala register
def generate_metadata(
    config_path: Path,
    metadata_root: Path,
    *,
    skip_confirmation: bool = False,
    generate_defaults: bool = False,
):
    parsed = parse_config(config_path)

    no_defaults: list[tuple[str, type, Field[object]]] = []
    defaults: list[tuple[str, type, Field[object]]] = []

    for k, (typ, default) in parsed.items():
        name = identifier_from_str(k)

        if not generate_defaults or default is NoValue():
            no_defaults.append((name, typ, field()))
            continue

        if isinstance(default, (list, dict, LatchFile, LatchDir)):
            defaults.append((name, typ, field(default_factory=lambda: default)))
            continue

        defaults.append((name, typ, field(default=default)))

    generated_args_type = make_dataclass("SnakemakeArgsType", no_defaults + defaults)

    if metadata_root.is_file():
        if not click.confirm(f"A file exists at `{metadata_root}`. Delete it?"):
            raise click.exceptions.Exit(0)

        metadata_root.unlink()

    metadata_root.mkdir(exist_ok=True)

    metadata_path = metadata_root / Path("__init__.py")

    if not metadata_path.exists():
        metadata_path.write_text(
            reindent(
                r"""
                from latch.types.metadata import LatchAuthor
                from latch.types.metadata.snakemake_v2 import SnakemakeV2Metadata, SnakemakeParameter

                from .generated import SnakemakeArgsType

                class WorkflowArgsType(SnakemakeArgsType):
                    # add custom parameters here
                    ...

                SnakemakeV2Metadata(
                    display_name="Your Workflow Name",
                    author=LatchAuthor(
                        name="Your Name",
                    ),
                    parameters={
                        "args": SnakemakeParameter(type=WorkflowArgsType)
                    },
                )
                """,
                0,
            )
        )
        click.secho(f"Generated `{metadata_path}`.", fg="green")

    params_path = metadata_root / Path("generated.py")
    if (
        params_path.exists()
        and not skip_confirmation
        and not click.confirm(f"File `{params_path}` already exists. Overwrite?")
    ):
        raise click.exceptions.Exit(0)

    params_path.write_text(
        reindent(
            rf"""
            # This file is auto-generated, PLEASE DO NOT EDIT DIRECTLY! To update, run
            #
            #   $ latch generate-metadata --snakemake {config_path}
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

            __preambles__

            """,
            0,
        ).replace("__preambles__", get_preamble(generated_args_type))
    )
    click.secho(f"Generated `{params_path}`.", fg="green")
