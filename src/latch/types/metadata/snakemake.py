from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Generic, Literal, Union

import click
from typing_extensions import TypeAlias

from latch_cli.snakemake.config.utils import validate_snakemake_type
from latch_cli.utils import identifier_suffix_from_str

from ..directory import LatchDir
from ..file import LatchFile
from .latch import LatchMetadata, LatchParameter
from .utils import P, ParameterType


@dataclass
class SnakemakeParameter(LatchParameter, Generic[P]):
    type: type[P] | None = None
    """
    The python type of the parameter.
    """
    default: P | None = None
    """
    Optional default value for this parameter
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

    def __post_init__(self):
        if self.type is None:
            click.secho("All SnakemakeParameter objects must specify a type.", fg="red")
            raise click.exceptions.Exit(1)


@dataclass
class SnakemakeFileParameter(SnakemakeParameter[Union[LatchFile, LatchDir]]):
    """Deprecated: use `file_metadata` keyword in `SnakemakeMetadata` instead"""

    type: type[LatchFile | LatchDir] | None = None
    """
    The python type of the parameter.
    """
    path: Path | None = None
    """
    The path where the file passed to this parameter will be copied.
    """
    config: bool = False
    """
    Whether or not the file path is exposed in the Snakemake config
    """
    download: bool = False
    """
    Whether or not the file is downloaded in the JIT step
    """


@dataclass
class SnakemakeFileMetadata:
    path: Path
    """
    The local path where the file passed to this parameter will be copied
    """
    config: bool = False
    """
    If `True`, expose the file in the Snakemake config
    """
    download: bool = False
    """
    If `True`, download the file in the JIT step
    """


@dataclass
class DockerMetadata:
    """Class describing credentials for private docker repositories"""

    username: str
    """
    The account username for the private repository
    """
    secret_name: str
    """
    The name of the Latch Secret that contains the password for the private repository
    """


@dataclass
class EnvironmentConfig:
    """Class describing environment for spawning Snakemake tasks"""

    use_conda: bool = False
    """
    Use Snakemake `conda` directive to spawn tasks in conda environments
    """
    use_container: bool = False
    """
    Use Snakemake `container` directive to spawn tasks in Docker containers
    """
    container_args: list[str] = field(default_factory=list)
    """
    Additional arguments to use when running Docker containers
    """


FileMetadata: TypeAlias = dict[str, Union[SnakemakeFileMetadata, "FileMetadata"]]


@dataclass
class SnakemakeMetadata(LatchMetadata):
    """Class for organizing Snakemake workflow metadata"""

    output_dir: LatchDir | None = None
    """
    Directory for snakemake workflow outputs
    """
    name: str | None = None
    """
    Name of the workflow
    """
    docker_metadata: DockerMetadata | None = None
    """
    Credentials configuration for private docker repositories
    """
    env_config: EnvironmentConfig = field(default_factory=EnvironmentConfig)
    """
    Environment configuration for spawning Snakemake tasks
    """
    parameters: dict[str, SnakemakeParameter[ParameterType]] = field(default_factory=dict)
    """
    A dictionary mapping parameter names (strings) to `SnakemakeParameter` objects
    """
    file_metadata: FileMetadata = field(default_factory=dict)
    """
    A dictionary mapping parameter names to `SnakemakeFileMetadata` objects
    """
    cores: int = 4
    """
    Number of cores to use for Snakemake tasks (equivalent of Snakemake's `--cores` flag)
    """
    about_page_content: Path | None = None
    """
    Path to a markdown file containing information about the pipeline - rendered in the About page.
    """

    def validate(self):
        if self.about_page_content is not None:
            if not isinstance(self.about_page_content, Path):
                click.secho(
                    f"`about_page_content` parameter ({self.about_page_content}) must"
                    " be a Path object.",
                    fg="red",
                )
                raise click.exceptions.Exit(1)

        for name, param in self.parameters.items():
            if param.default is None:
                continue
            try:
                validate_snakemake_type(name, param.type, param.default)
            except ValueError as e:
                click.secho(e, fg="red")
                raise click.exceptions.Exit(1) from e

    def __post_init__(self):
        self.validate()

        if self.name is None:
            self.name = f"snakemake_{identifier_suffix_from_str(self.display_name.lower())}"

        global _snakemake_metadata
        _snakemake_metadata = self

    @property
    def dict(self):
        d = super().dict
        # ayush: Paths aren't JSON serializable but ribosome doesn't need it anyway so we can just delete it
        del d["__metadata__"]["about_page_content"]
        return d


_snakemake_metadata: SnakemakeMetadata | None = None
