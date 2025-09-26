from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from latch_cli.utils import identifier_suffix_from_str

from .latch import LatchMetadata

if TYPE_CHECKING:
    from .snakemake import SnakemakeParameter


@dataclass(frozen=True)
class SnakemakeRuntimeResources:
    """Resources for Snakemake runtime tasks"""

    cpus: int = 1
    """
    Number of CPUs required for the task
    """
    memory: int = 2
    """
    Memory required for the task in GiB
    """
    storage_gib: int = 50
    """
    Storage required for the task in GiB
    """


@dataclass
class SnakemakeV2Metadata(LatchMetadata):
    parameters: dict[str, SnakemakeParameter[Any]] = field(default_factory=dict)
    """
    A dictionary mapping parameter names (strings) to `SnakemakeParameter` objects
    """
    about_page_path: Path | None = None
    """
    Path to a markdown file containing information about the pipeline - rendered in the About page.
    """
    runtime_resources: SnakemakeRuntimeResources = field(default_factory=SnakemakeRuntimeResources)

    def validate(self):
        if self.about_page_path is not None and not isinstance(self.about_page_path, Path):
            click.secho(
                f"SnakemakeV2Metadata.about_page_path ({self.about_page_path}) must be a"
                " `Path` object.",
                fg="red",
            )
            raise click.exceptions.Exit(1)

    def __post_init__(self):
        self.validate()

        self.name = identifier_suffix_from_str(f"snakemake_v2_{self.display_name}".lower())

        global _snakemake_v2_metadata
        _snakemake_v2_metadata = self

    @property
    def dict(self):
        d = super().dict
        del d["__metadata__"]["about_page_path"]
        return d


_snakemake_v2_metadata: SnakemakeV2Metadata | None = None
