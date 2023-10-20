from pathlib import Path
from typing import Dict, List, Tuple, Type, TypeVar

import click
import yaml

from latch_cli.snakemake.workflow import reindent
from latch_cli.utils import identifier_from_str

from .utils import JSONValue, get_preamble, parse_type, parse_value, type_repr

T = TypeVar("T")


def parse_config(config_path: Path) -> Dict[str, Tuple[Type[T], T]]:
    if not config_path.exists():
        click.secho(
            f"No config file found at {config_path}.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if config_path.is_dir():
        click.secho(
            f"Path {config_path} points to a directory.",
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
        # ayush: this case doesn't matter bc a non-dict .yaml file isn't valid snakemake
        return {"snakemake_parameter": (parse_type(res), res)}

    parsed: Dict[str, Type] = {}
    for k, v in res.items():
        typ = parse_type(v, k)
        val = parse_value(typ, v)

        parsed[k] = (typ, val)

    return parsed


def generate_metadata(
    config_path: Path,
    *,
    skip_confirmation: bool = False,
    generate_defaults: bool = False,
):
    parsed = parse_config(config_path)

    preambles: List[str] = []
    params: List[str] = []

    for k, (typ, val) in parsed.items():
        preambles.append(get_preamble(typ))

        param = reindent(
            f"""\
            {repr(identifier_from_str(k))}: SnakemakeParameter(
                display_name={repr(k)},
                type={type_repr(typ)},
            __default__),""",
            0,
        )

        default = ""
        if generate_defaults:
            default = f"    default={repr(val)},\n"

        param = param.replace("__default__", default)
        param = reindent(param, 1)

        params.append(param)

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
        # todo(ayush): seems like python path-based import hooks seem to choose the
        # package latch_metadata/__init__.py over latch_metadata.py
        #
        # Couldn't find this documented anywhere - is this a CPython implementation detail?
        click.secho(
            "Warning: Found both `latch_metadata.py` and `latch_metadata/__init__.py`"
            " in current directory. `latch_metadata.py` will be ignored.",
            fg="yellow",
        )

    if not metadata_path.exists() and click.confirm(
        "Could not find an `__init__.py` file in `latch_metadata`. Generate one?"
    ):
        metadata_path.write_text(
            reindent(
                r"""
                from latch.types.metadata import SnakemakeMetadata, LatchAuthor
                from latch.types.directory import LatchDir

                from .parameters import generated_parameters

                SnakemakeMetadata(
                    output_dir=LatchDir("latch:///your_output_directory"),
                    display_name="Your Workflow Name",
                    author=LatchAuthor(
                        name="Your Name",
                    ),
                    # Add more parameters
                    parameters=generated_parameters,
                )
                """,
                0,
            )
        )

    params_path = metadata_root / Path("parameters.py")
    if (
        params_path.exists()
        and not skip_confirmation
        and not click.confirm(
            "File `latch_metadata/parameters.py` already exists. Overwrite?"
        )
    ):
        raise click.exceptions.Exit(0)

    params_path.write_text(
        reindent(
            r"""
            from dataclasses import dataclass
            import typing

            from latch.types.metadata import SnakemakeParameter

            __preambles__

            # Import these into your `__init__.py` file:
            #
            # from .parameters import generated_parameters
            #
            generated_parameters = {
            __params__
            }
            """,
            0,
        )
        .replace("__preambles__", "".join(preambles))
        .replace("__params__", "\n".join(params))
    )
