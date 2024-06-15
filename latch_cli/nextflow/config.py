import json
from pathlib import Path
from textwrap import dedent
from typing import Any, Optional, Type

import click

from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile
from latch_cli.snakemake.config.utils import type_repr
from latch_cli.snakemake.utils import reindent


def get_param_type(details: dict) -> Type:
    t = details.get("type")
    if t is None:
        return Any

    if t == "string":
        format = details.get("format")
        if format is not None:
            if format == "file-path":
                return LatchFile
            elif format == "directory-path":
                return LatchDir
        return str
    elif t == "boolean":
        return bool
    elif t == "integer":
        return int
    elif t == "number":
        return float

    return Any


def generate_metadata(
    schema_path: Path,
    metadata_root: Path,
    *,
    skip_confirmation: bool = False,
    generate_defaults: bool = False,
):
    schema: dict = json.loads(schema_path.read_text())

    display_name: Optional[str] = schema.get("title")
    # rahul: seems like it is convention to use "<pipeline-name> pipeline parameters" for schema title
    suffix = " pipeline parameters"
    if display_name is not None and display_name.endswith(suffix):
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
        metadata_path.write_text(dedent(f"""
            from latch.types.metadata import (
                NextflowMetadata,
                LatchAuthor,
                NextflowRuntimeResources
            )
            from latch.types.directory import LatchDir

            from .parameters import generated_parameters

            NextflowMetadata(
                display_name={repr(display_name)},
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
        """))
        click.secho(f"Generated `{metadata_path}`.", fg="green")

    params = []
    for section in schema.get("definitions", {}).values():
        section_title = section.get("title")
        required_params = set(section.get("required", []))

        first = True
        for param, details in section.get("properties", {}).items():
            if details.get("hidden", False):
                continue

            if first:
                first = False
            else:
                section_title = None

            t = LatchOutputDir if param == "outdir" else get_param_type(details)

            default = None
            if generate_defaults and t not in {LatchFile, LatchDir, LatchOutputDir}:
                default = details.get("default")

            if param not in required_params:
                t = Optional[t]

            desc = details.get("description")

            params.append(dedent(f"""
                    {repr(param)}: NextflowParameter(
                        type={type_repr(t)},
                        default={repr(default)},
                        section_title={repr(section_title)},
                        description={repr(desc)},
                    ),"""))

    params_path = metadata_root / "parameters.py"
    if (
        params_path.exists()
        and not skip_confirmation
        and not click.confirm(f"File `{params_path}` already exists. Overwrite?")
    ):
        return

    params_path.write_text(dedent(r"""
            from dataclasses import dataclass
            import typing
            import typing_extensions

            from flytekit.core.annotation import FlyteAnnotation

            from latch.types.metadata import NextflowParameter
            from latch.types.file import LatchFile
            from latch.types.directory import LatchDir, LatchOutputDir

            # Import these into your `__init__.py` file:
            #
            # from .parameters import generated_parameters

            generated_parameters = {
            __params__
            }

            """).replace("__params__", reindent("".join(params), 1)))
    click.secho(f"Generated `{params_path}`.", fg="green")
