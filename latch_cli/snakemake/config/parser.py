from pathlib import Path
from typing import Dict, Type

import click
import yaml

from latch_cli.snakemake.workflow import reindent
from latch_cli.utils import identifier_suffix_from_str

from .utils import *


def parse_config(config_path: Path) -> Dict[str, Type]:
    if not config_path.exists():
        click.secho(
            reindent(
                f"""
                No config file found at {config_path}.
                """,
                0,
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if config_path.is_dir():
        click.secho(
            reindent(
                f"""
                Path {config_path} points to a directory.
                """,
                0,
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    try:
        res: JSONValue = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError as e:
        click.secho(
            reindent(
                f"""
                Error loading config from {config_path}:

                {e}
                """,
                0,
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    if not isinstance(res, dict):
        # todo(ayush): think more about correct behavior for pathological .yaml files
        return {"snakemake_parameter": res}

    parsed: Dict[str, Type] = {}
    for k, v in res.items():
        parsed[k] = parse_type(v, identifier_suffix_from_str(k))

    return parsed


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

    metadata_path = Path("latch_metadata.py")
    if metadata_path.exists() and not click.confirm(
        "File `latch_metadata.py` already exists. Overwrite?"
    ):
        return

    metadata_path.write_text(
        reindent(
            f"""
            from dataclasses import dataclass
            import typing

            from latch.types.metadata import SnakemakeParameter, SnakemakeMetadata, LatchAuthor
            from latch.types.directory import LatchDir
            from latch.types.file import LatchFile

            __preambles__
            SnakemakeMetadata(
                output_dir=LatchDir("latch:///your_output_directory"),
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
