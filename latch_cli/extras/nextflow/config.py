import json
import os
import subprocess
from pathlib import Path
from typing import List, Optional

import click

from ...utils import best_effort_display_name, identifier_from_str
from ..common.config.parser import parse_config, write_metadata
from ..common.config.utils import JSONValue, get_preamble, type_repr
from ..common.utils import reindent
from .build import ensure_nf_dependencies

nextflow_metadata = """
from latch.types.metadata import NextflowMetadata, LatchAuthor, EnvironmentConfig
from latch.types.directory import LatchDir

from .parameters import generated_parameters

NextflowMetadata(
    name="Your Workflow Name",
    display_name="Your Workflow Name",
    author=LatchAuthor(
        name="Your Name",
    ),
    parameters=generated_parameters,
    output_directory=LatchDir("latch:///your_output_directory"),
)

"""


def generate_nf_metadata(
    config_path: Path,
    *,
    skip_confirmation: bool = False,
    generate_defaults: bool = False,
    infer_files: bool = False,
    make_optionals: bool = True,
):
    if not config_path.is_dir():
        click.secho(
            f"Nextflow config path must point to the package root directory.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    ensure_nf_dependencies(config_path)

    env = {
        **os.environ,
        # read NF binaries from `.latch/.nextflow` instead of system
        "NXF_HOME": str(config_path / ".latch" / ".nextflow"),
        # don't display version mismatch warning
        "NXF_DISABLE_CHECK_LATEST": "true",
        # don't emit .nextflow.log files
        "NXF_LOG_FILE": "/dev/null",
    }

    config_file = ".latch/nf-config.yaml"
    try:
        subprocess.run(
            [
                ".latch/bin/nextflow",
                "config",
                "-format",
                "json",
                "-output",
                config_file,
            ],
            check=True,
            env=None if os.environ.get("LATCH_NEXTFLOW_DEV") is not None else env,
            cwd=config_path,
        )
    except subprocess.CalledProcessError as e:
        click.secho(
            "Failed to read config from nextflow package.",
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    config_file = config_path / config_file

    try:
        config: JSONValue = json.loads(config_file.read_text())
    except json.JSONDecodeError as e:
        click.secho(
            reindent(
                f"""
                Error loading config from {config_file}:

                {e}
                """,
                0,
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    parsed = parse_config(config.get("params", {}), infer_files=infer_files)

    preambles: List[str] = []
    params: List[str] = []

    for k, (typ, (val, default)) in parsed.items():
        if make_optionals:
            typ = Optional[typ]

        preambles.append(get_preamble(typ, make_optionals=make_optionals))

        param_str = reindent(
            f"""\
            {repr(identifier_from_str(k))}: NextflowParameter(
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

    params_file_str = (
        reindent(
            r"""
            from dataclasses import dataclass
            import typing
            import typing_extensions

            from flytekit.core.annotation import FlyteAnnotation

            from latch.types.metadata import NextflowParameter
            from latch.types.file import LatchFile
            from latch.types.directory import LatchDir

            ||preambles||

            # Import these into your `__init__.py` file:
            #
            # from .parameters import generated_parameters, file_metadata

            generated_parameters = {
            ||params||
            }

            """,
            0,
        )
        .replace("||preambles||", "".join(preambles))
        .replace("||params||", "\n".join(params))
    )

    write_metadata(
        nextflow_metadata, params_file_str, skip_confirmation=skip_confirmation
    )
