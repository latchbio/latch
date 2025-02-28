from __future__ import annotations

import csv
import functools
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Generic, Literal, get_args, get_origin

import click

from latch_cli.utils import identifier_suffix_from_str

from ..directory import LatchDir, LatchOutputDir
from ..file import LatchFile
from .latch import LatchMetadata, LatchParameter
from .utils import DC, P


@dataclass
class NextflowParameter(Generic[P], LatchParameter):
    type: type[P] | None = None
    """
    The python type of the parameter.
    """
    default: P | None = None
    """
    Default value of the parameter
    """

    samplesheet_type: Literal["csv", "tsv", None] = None
    """
    The type of samplesheet to construct from the input parameter.

    Only used if the provided parameter is a samplesheet (samplesheet=True)
    """
    samplesheet_constructor: Callable[[P], Path] | None = None
    """
    A custom samplesheet constructor.

    Should return the path of the constructed samplesheet. If samplesheet_type is also specified, this takes precedence.
    Only used if the provided parameter is a samplesheet (samplesheet=True)
    """
    results_paths: list[Path] | None = None
    """
    Output sub-paths that will be exposed in the UI under the "Results" tab on the workflow execution page.

    Only valid where the `type` attribute is a LatchDir
    """

    def __post_init__(self):
        if self.results_paths is not None and self.type not in {
            LatchDir,
            LatchOutputDir,
        }:
            click.secho(
                "`results_paths` attribute can only be defined for parameters"
                " of type `LatchDir`.",
                fg="red",
            )
            raise click.exceptions.Exit(1)

        if not self.samplesheet or self.samplesheet_constructor is not None:
            return

        t = self.type
        if get_origin(t) is not list or not is_dataclass(get_args(t)[0]):
            click.secho("Samplesheets must be a list of dataclasses.", fg="red")
            raise click.exceptions.Exit(1)

        if self.samplesheet_type is not None:
            delim = "," if self.samplesheet_type == "csv" else "\t"
            self.samplesheet_constructor = functools.partial(
                _samplesheet_constructor, t=get_args(self.type)[0], delim=delim
            )
            return

        click.secho(
            dedent("""\
            A Samplesheet constructor is required for a samplesheet parameter. Please either provide a value for
            `samplesheet_type` or provide a custom callable to the `samplesheet_constructor` argument.
            """),
            fg="red",
        )
        raise click.exceptions.Exit(1)


def _samplesheet_repr(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (LatchFile, LatchDir)):
        return str(v.remote_path)
    if isinstance(v, Enum):
        return v.value

    return str(v)


def _samplesheet_constructor(samples: list[DC], t: DC, delim: str = ",") -> Path:
    samplesheet = Path("samplesheet.csv")

    with samplesheet.open("w") as f:
        writer = csv.DictWriter(f, [f.name for f in fields(t)], delimiter=delim)
        writer.writeheader()

        for sample in samples:
            row_data = {
                f.name: _samplesheet_repr(getattr(sample, f.name))
                for f in fields(sample)
            }
            writer.writerow(row_data)

    return samplesheet


@dataclass(frozen=True)
class NextflowRuntimeResources:
    """Resources for Nextflow runtime tasks"""

    cpus: int | None = 4
    """
    Number of CPUs required for the task
    """
    memory: int | None = 8
    """
    Memory required for the task in GiB
    """
    storage_gib: int | None = 100
    """
    Storage required for the task in GiB
    """
    storage_expiration_hours: int = 7 * 24
    """
    Number of hours after execution failure that workdir should be retained in EFS.
    Warning: Increasing this number will increase your Nextflow Storage costs.
    """


@dataclass
class NextflowMetadata(LatchMetadata):
    name: str | None = None
    """
    Name of the workflow
    """
    parameters: dict[str, NextflowParameter[Any]] = field(default_factory=dict)
    """
    A dictionary mapping parameter names (strings) to `NextflowParameter` objects
    """
    runtime_resources: NextflowRuntimeResources = field(
        default_factory=NextflowRuntimeResources
    )
    """
    Resources (cpu/memory/storage) for Nextflow runtime task
    """
    execution_profiles: list[str] = field(default_factory=list)
    """
    Execution config profiles to expose to users in the Latch console
    """
    log_dir: LatchDir | None = None
    """
    Directory to dump Nextflow logs
    """
    upload_command_logs: bool = False
    """
    Upload .command.* logs to Latch Data after each task execution
    """
    about_page_path: Path | None = None
    """
    Path to a markdown file containing information about the pipeline - rendered in the About page.
    """

    def validate(self):
        if self.about_page_path is not None and not isinstance(
            self.about_page_path, Path
        ):  # type: ignore
            click.secho(
                f"`about_page_path` parameter ({self.about_page_path}) must be a"
                " Path object.",
                fg="red",
            )

    @property
    def dict(self):
        d = super().dict
        del d["__metadata__"]["about_page_path"]
        return d

    def __post_init__(self):
        self.validate()

        if self.name is None:
            if self.display_name is None:
                click.secho(
                    "Name or display_name must be provided in metadata", fg="red"
                )
            self.name = f"nf_{identifier_suffix_from_str(self.display_name.lower())}"
        else:
            self.name = identifier_suffix_from_str(self.name)

        global _nextflow_metadata
        _nextflow_metadata = self


_nextflow_metadata: NextflowMetadata | None = None
