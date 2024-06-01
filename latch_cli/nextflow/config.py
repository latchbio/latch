import json
from pathlib import Path
from textwrap import dedent
from typing import Any, Optional, Type

import click

from latch.types.directory import LatchDir
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

    return Any


def parse_config(pkg_root: Path):
    schema_f = pkg_root / "nextflow_schema.json"
    schema: dict = json.loads(schema_f.read_text())

    display_name: Optional[str] = schema.get("title")
    # rahul: seems like it is convention to use "<pipeline-name> pipeline parameters" for schema title
    suffix = " pipeline parameters"
    if display_name is not None and display_name.endswith(suffix):
        display_name = display_name[: -len(suffix)]

    metadata_root = pkg_root / "latch_metadata"
    if metadata_root.is_file():
        if not click.confirm("A file exists at `latch_metadata`. Delete it?"):
            raise click.exceptions.Exit(0)

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
                    disk=100,
                ),
                log_dir=LatchDir("latch:///your_log_dir"),
            )
        """))
        click.secho("Generated `latch_metadata/__init__.py`.", fg="green")

    params = []
    for section in schema.get("definitions", {}).values():
        section_title = section.get("title")
        required_params = set(section.get("required", []))

        for param, details in section.get("properties", {}).items():
            if details.get("hidden", False):
                continue

            t = get_param_type(details)
            if param not in required_params:
                t = Optional[t]

            default = None
            if t not in {LatchFile, LatchDir}:
                default = details.get("default")

            desc = details.get("description")

            params.append(dedent(f"""
                    {repr(param)}: NextflowParameter(
                        type={type_repr(t)},
                        default={repr(default)},
                        section_title={repr(section_title)},
                        description={repr(desc)},
                    ),"""))

    params_path = metadata_root / "parameters.py"
    if params_path.exists() and not click.confirm(
        "File `latch_metadata/parameters.py` already exists. Overwrite?"
    ):
        raise click.exceptions.Exit(0)

    params_path.write_text(dedent(r"""
            from dataclasses import dataclass
            import typing
            import typing_extensions

            from flytekit.core.annotation import FlyteAnnotation

            from latch.types.metadata import NextflowParameter, SnakemakeFileParameter
            from latch.types.file import LatchFile
            from latch.types.directory import LatchDir

            # Import these into your `__init__.py` file:
            #
            # from .parameters import generated_parameters, file_metadata

            generated_parameters = {
            __params__
            }

            """).replace("__params__", reindent("".join(params), 1)))
    click.secho("Generated `latch_metadata/parameters.py`.", fg="green")


if __name__ == "__main__":
    parse_config(Path("."))
