from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, List, Type, Union, get_args, get_origin

import click
import yaml
from typing_extensions import Annotated

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.utils import best_effort_display_name, identifier_from_str

from ..common.config.parser import parse_config, write_metadata
from ..common.config.utils import (
    JSONValue,
    get_preamble,
    is_list_type,
    is_primitive_type,
    type_repr,
)
from ..common.utils import reindent

snakemake_metadata = """
from latch.types.metadata import SnakemakeMetadata, LatchAuthor, EnvironmentConfig
from latch.types.directory import LatchDir

from .parameters import generated_parameters, file_metadata

SnakemakeMetadata(
    output_dir=LatchDir("latch:///your_output_directory"),
    display_name="Your Workflow Name",
    author=LatchAuthor(
        name="Your Name",
    ),
    env_config=EnvironmentConfig(
        use_conda=False,
        use_container=False,
    ),
    cores=4,
    parameters=generated_parameters,
    file_metadata=file_metadata,
)

"""


def file_metadata_str(typ: Type, value: JSONValue, level: int = 0) -> str:
    if get_origin(typ) is Annotated:
        args = get_args(typ)
        assert len(args) > 0
        return file_metadata_str(args[0], value, level)

    if is_primitive_type(typ):
        return ""

    if typ in {LatchFile, LatchDir}:
        return reindent(
            f"""\
            SnakemakeFileMetadata(
                path={repr(value)},
                config=True,
            ),\n""",
            level,
        )

    metadata: List[str] = []
    if is_list_type(typ):
        template = """
        [
        __metadata__],\n"""

        args = get_args(typ)
        assert len(args) > 0
        for val in value:
            metadata_str = file_metadata_str(get_args(typ)[0], val, level + 1)
            if metadata_str == "":
                continue
            metadata.append(metadata_str)
    else:
        template = """
        {
        __metadata__},\n"""

        assert is_dataclass(typ)
        for field in fields(typ):
            metadata_str = file_metadata_str(
                field.type, getattr(value, field.name), level
            )
            if metadata_str == "":
                continue
            metadata_str = f"{repr(identifier_from_str(field.name))}: {metadata_str}"
            metadata.append(reindent(metadata_str, level + 1))

    if len(metadata) == 0:
        return ""

    return reindent(
        template,
        level,
    ).replace("__metadata__", "".join(metadata), level + 1)


# todo(ayush): print informative stuff here ala register
def generate_snakemake_metadata(
    config_path: Path,
    *,
    skip_confirmation: bool = False,
    generate_defaults: bool = False,
    infer_files: bool = False,
):
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
        config: JSONValue = yaml.safe_load(config_path.read_text())
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

    parsed = parse_config(config, infer_files=infer_files)

    preambles: List[str] = []
    params: List[str] = []
    file_metadata: List[str] = []

    for k, (typ, (val, default)) in parsed.items():
        preambles.append(get_preamble(typ))

        param_str = reindent(
            f"""\
            {repr(identifier_from_str(k))}: SnakemakeParameter(
                display_name={repr(best_effort_display_name(k))},
                type={type_repr(typ)},
            __default__),""",
            0,
        )

        default_str = ""
        if generate_defaults and default is not None:
            default_str = f"    default={repr(default)},\n"

        param_str = param_str.replace("__default__", default_str)

        param_str = reindent(param_str, 1)
        params.append(param_str)

        metadata_str = file_metadata_str(typ, val)
        if metadata_str == "":
            continue
        metadata_str = f"{repr(identifier_from_str(k))}: {metadata_str}"
        file_metadata.append(reindent(metadata_str, 1))

    params_file_str = (
        reindent(
            r"""
            from dataclasses import dataclass
            import typing
            import typing_extensions

            from flytekit.core.annotation import FlyteAnnotation

            from latch.types.metadata import SnakemakeParameter, SnakemakeFileParameter, SnakemakeFileMetadata
            from latch.types.file import LatchFile
            from latch.types.directory import LatchDir

            __preambles__

            # Import these into your `__init__.py` file:
            #
            # from .parameters import generated_parameters, file_metadata

            generated_parameters = {
            __params__
            }

            file_metadata = {
            __file_metadata__}

            """,
            0,
        )
        .replace("__preambles__", "".join(preambles))
        .replace("__params__", "\n".join(params))
        .replace("__file_metadata__", "".join(file_metadata))
    )

    write_metadata(
        snakemake_metadata, params_file_str, skip_confirmation=skip_confirmation
    )


def validate_snakemake_type(name: str, t: Type, param: Any) -> None:
    if t is type(None):
        return param is None

    elif is_primitive_type(t) or t in {LatchFile, LatchDir}:
        if param is None:
            raise ValueError(
                f"Parameter {name} of type {t} cannot be None. Either specify a"
                " non-None default value or use the Optional type"
            )
        if not isinstance(param, t):
            raise ValueError(f"Parameter {name} must be of type {t}, not {type(param)}")

    elif get_origin(t) is Union:
        args = get_args(t)
        # only Optional types supported
        if len(args) != 2 or args[1] is not type(None):
            raise ValueError(
                f"Failed to parse input param {param}. Union types other than"
                " Optional are not yet supported in Snakemake workflows."
            )
        if param is None:
            return
        validate_snakemake_type(name, args[0], param)

    elif get_origin(t) is Annotated:
        args = get_args(t)
        assert len(args) > 0
        validate_snakemake_type(name, args[0], param)

    elif is_list_type(t):
        args = get_args(t)
        if len(args) == 0:
            raise ValueError(
                "Generic Lists are not supported - please specify a subtype,"
                " e.g. List[LatchFile]",
            )
        list_typ = args[0]
        for i, val in enumerate(param):
            validate_snakemake_type(f"{name}[{i}]", list_typ, val)

    else:
        assert is_dataclass(t)
        for field in fields(t):
            validate_snakemake_type(
                f"{name}.{field.name}", field.type, getattr(param, field.name)
            )
