from pathlib import Path
from typing import Dict, Tuple, Type, TypeVar

import click

from ..utils import reindent
from .utils import JSONValue, parse_type, parse_value

T = TypeVar("T")


def parse_config(
    config: JSONValue,
    *,
    infer_files: bool = False,
) -> Dict[str, Tuple[Type[T], T]]:
    parsed: Dict[str, Type] = {}
    for k, v in config.items():
        try:
            typ = parse_type(v, k, infer_files=infer_files)
        except ValueError as e:
            click.secho(
                f"WARNING: Skipping parameter {k}. Failed to parse type: {e}.",
                fg="yellow",
            )
            continue
        val, default = parse_value(typ, v)

        parsed[k] = (typ, (val, default))

    return parsed


def write_metadata(
    metadata: str,
    params: str,
    *,
    skip_confirmation: bool = False,
) -> None:
    metadata_root = Path("latch_metadata")
    if metadata_root.is_file():
        if not click.confirm("A file exists at `latch_metadata`. Delete it?"):
            raise click.exceptions.Exit(0)

        metadata_root.unlink()

    metadata_root.mkdir(exist_ok=True)

    metadata_path = metadata_root / Path("__init__.py")
    old_metadata_path = Path("latch_metadata.py")

    if old_metadata_path.exists() and not metadata_path.exists():
        if click.confirm(
            "Found legacy `latch_metadata.py` file in current directory. This is"
            " deprecated and will be ignored in future releases. Move to"
            " `latch_metadata/__init__.py`? (This will not change file contents)"
        ):
            old_metadata_path.rename(metadata_path)
    elif old_metadata_path.exists() and metadata_path.exists():
        click.secho(
            "Warning: Found both `latch_metadata.py` and"
            " `latch_metadata/__init__.py` in current directory."
            " `latch_metadata.py` will be ignored.",
            fg="yellow",
        )

    if not metadata_path.exists():
        metadata_path.write_text(reindent(metadata, 0))
        click.secho("Generated `latch_metadata/__init__.py`.", fg="green")

    params_path = metadata_root / Path("parameters.py")
    if (
        params_path.exists()
        and not skip_confirmation
        and not click.confirm(
            "File `latch_metadata/parameters.py` already exists. Overwrite?"
        )
    ):
        raise click.exceptions.Exit(0)

    params_path.write_text(reindent(params, 0))
    click.secho("Generated `latch_metadata/parameters.py`.", fg="green")
