import csv
import functools
import re
from dataclasses import Field, asdict, dataclass, field, fields, is_dataclass
from enum import Enum
from pathlib import Path
from textwrap import dedent, indent
from typing import (
    Any,
    Callable,
    ClassVar,
    Collection,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

import click
import yaml
from typing_extensions import TypeAlias

from latch_cli.snakemake.config.utils import validate_snakemake_type
from latch_cli.utils import identifier_suffix_from_str

from .directory import LatchDir, LatchOutputDir
from .file import LatchFile


@dataclass
class LatchRule:
    """Class describing a rule that a parameter input must follow"""

    regex: str
    """A string regular expression which inputs must match"""
    message: str
    """The message to render when an input does not match the regex"""

    @property
    def dict(self):
        return asdict(self)

    def __post_init__(self):
        try:
            re.compile(self.regex)
        except re.error as e:
            raise ValueError(f"Malformed regex {self.regex}: {e.msg}")


class LatchAppearanceEnum(Enum):
    line = "line"
    paragraph = "paragraph"


@dataclass(frozen=True)
class MultiselectOption:
    name: str
    value: object


@dataclass(frozen=True)
class Multiselect:
    options: List[MultiselectOption] = field(default_factory=list)
    allow_custom: bool = False


# backwards compatibility
LatchAppearanceType = LatchAppearanceEnum

LatchAppearance: TypeAlias = Union[LatchAppearanceEnum, Multiselect]


@dataclass
class LatchAuthor:
    """Class describing metadata about the workflow author"""

    name: Optional[str] = None
    """The name of the author"""
    email: Optional[str] = None
    """The email of the author"""
    github: Optional[str] = None
    """A link to the github profile of the author"""


@dataclass(frozen=True)
class FlowBase:
    """Parent class for all flow elements

    Available flow elements:

    * :class:`~latch.types.metadata.Params`

    * :class:`~latch.types.metadata.Text`

    * :class:`~latch.types.metadata.Title`

    * :class:`~latch.types.metadata.Section`

    * :class:`~latch.types.metadata.Spoiler`

    * :class:`~latch.types.metadata.Fork`
    """

    ...


@dataclass(frozen=True, init=False)
class Section(FlowBase):
    """Flow element that displays a child flow in a card with a given title

    Example:


    .. image:: ../assets/flow-example/flow_example_1.png
        :alt: Example of a user interface for a workflow with a custom flow

    .. image:: ../assets/flow-example/flow_example_spoiler.png
        :alt: Example of a spoiler flow element


    The `LatchMetadata` for the example above can be defined as follows:

    .. code-block:: python

        from latch.types import LatchMetadata, LatchParameter
        from latch.types.metadata import FlowBase, Section, Text, Params, Fork, Spoiler
        from latch import workflow

        flow = [
            Section(
                "Samples",
                Text(
                    "Sample provided has to include an identifier for the sample (Sample name)"
                    " and one or two files corresponding to the reads (single-end or paired-end, respectively)"
                ),
                Fork(
                    "sample_fork",
                    "Choose read type",
                    paired_end=ForkBranch("Paired-end", Params("paired_end")),
                    single_end=ForkBranch("Single-end", Params("single_end")),
                ),
            ),
            Section(
                "Quality threshold",
                Text(
                    "Select the quality value in which a base is qualified."
                    "Quality value refers to a Phred quality score"
                ),
                Params("quality_threshold"),
            ),
            Spoiler(
                "Output directory",
                Text("Name of the output directory to send results to."),
                Params("output_directory"),
            ),
        ]

        metadata = LatchMetadata(
            display_name="fastp - Flow Tutorial",
            author=LatchAuthor(
                name="LatchBio",
            ),
            parameters={
                "sample_fork": LatchParameter(),
                "paired_end": LatchParameter(
                    display_name="Paired-end reads",
                    description="FASTQ files",
                    batch_table_column=True,
                ),
                "single_end": LatchParameter(
                    display_name="Single-end reads",
                    description="FASTQ files",
                    batch_table_column=True,
                ),
                "output_directory": LatchParameter(
                    display_name="Output directory",
                ),
            },
            flow=flow,
        )

        @workflow(metadata)
        def fastp(
            sample_fork: str,
            paired_end: PairedEnd,
            single_end: Optional[SingleEnd] = None,
            output_directory: str = "fastp_results",
        ) -> LatchDir:
            ...
    """

    section: str
    """Title of the section"""
    flow: List[FlowBase]
    """Flow displayed in the section card"""

    def __init__(self, section: str, *flow: FlowBase):
        object.__setattr__(self, "section", section)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True)
class Text(FlowBase):
    """Flow element that displays a markdown string"""

    text: str
    """Markdown body text"""


@dataclass(frozen=True)
class Title(FlowBase):
    """Flow element that displays a markdown title"""

    title: str
    """Markdown title text"""


@dataclass(frozen=True, init=False)
class Params(FlowBase):
    """Flow element that displays parameter widgets"""

    params: List[str]
    """
    Names of parameters whose widgets will be displayed.
    Order is preserved. Duplicates are allowed
    """

    def __init__(self, *args: str):
        object.__setattr__(self, "params", list(args))


@dataclass(frozen=True, init=False)
class Spoiler(FlowBase):
    """Flow element that displays a collapsible card with a given title"""

    spoiler: str
    """Title of the spoiler"""
    flow: List[FlowBase]
    """Flow displayed in the spoiler card"""

    def __init__(self, spoiler: str, *flow: FlowBase):
        object.__setattr__(self, "spoiler", spoiler)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class ForkBranch:
    """Definition of a :class:`~latch.types.metadata.Fork` branch"""

    display_name: str
    """String displayed in the fork's multibutton"""
    flow: List[FlowBase]
    """Child flow displayed in the fork card when the branch is active"""

    def __init__(self, display_name: str, *flow: FlowBase):
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class Fork(FlowBase):
    """Flow element that displays a set of mutually exclusive alternatives

    Displays a title, followed by a horizontal multibutton for selecting a branch,
    then a card for the active branch
    """

    fork: str
    """Name of a `str`-typed parameter to store the active branch's key"""
    display_name: str
    """Title shown above the fork selector"""
    flows: Dict[str, ForkBranch]
    """
    Mapping between branch keys to branch definitions.
    Order determines the order of options in the multibutton
    """

    def __init__(self, fork: str, display_name: str, **flows: ForkBranch):
        object.__setattr__(self, "fork", fork)
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flows", flows)


@dataclass
class LatchParameter:
    """Class for organizing parameter metadata"""

    display_name: Optional[str] = None
    """The name used to display the parameter on Latch Console"""
    description: Optional[str] = None
    """The description of the parameter's role in the workflow"""
    hidden: bool = False
    """Whether or not the parameter should be hidden by default"""
    section_title: Optional[str] = None
    """Whether this parameter should start a new section"""
    placeholder: Optional[str] = None
    """
    What should be rendered as a placeholder in the input box
    of the parameter before any value is inputed.
    """
    comment: Optional[str] = None
    """Any comment on the parameter itself"""
    output: bool = False
    """
    Whether or not this parameter is an output (used to disable
    path validation before launching a workflow)
    """
    batch_table_column: bool = False
    """
    Whether this parameter should be given a column in the batch
    table at the top of the workflow inputs
    """
    allow_dir: bool = True
    """
    Whether or not this parameter should accept directories in UI
    """
    allow_file: bool = True
    """
    Whether or not this parameter should accept files in UI.
    """
    appearance_type: LatchAppearance = LatchAppearanceEnum.line
    """
    Whether the parameter should be rendered as a line or paragraph
    (must be exactly one of either LatchAppearanceType.line or
    LatchAppearanceType.paragraph)
    """
    rules: List[LatchRule] = field(default_factory=list)
    """
    A list of LatchRule objects that inputs to this parameter must follow
    """
    detail: Optional[str] = None
    samplesheet: Optional[bool] = None
    """
    Use samplesheet input UI. Allows importing from Latch Registry.
    Parameter type must be a list of dataclasses
    """
    allowed_tables: Optional[List[int]] = None
    """
    If using the samplesheet component, specify a set of Registry Tables (by ID) to allow selection from.
    If not provided, all Tables are allowed.

    Only has an effect if `samplesheet=True`.
    """
    _custom_ingestion: Optional[str] = None

    def __str__(self):
        metadata_yaml = yaml.safe_dump(self.dict, sort_keys=False)
        if self.description is not None:
            return f"{self.description}\n{metadata_yaml}"
        return metadata_yaml

    @property
    def dict(self):
        parameter_dict: Dict[str, Any] = {"display_name": self.display_name}

        if self.output:
            parameter_dict["output"] = True
        if self.batch_table_column:
            parameter_dict["batch_table_column"] = True
        if self.samplesheet:
            parameter_dict["samplesheet"] = True
            if self.allowed_tables is not None:
                parameter_dict["allowed_tables"] = [str(x) for x in self.allowed_tables]

        temp_dict: Dict[str, Any] = {"hidden": self.hidden}
        if self.section_title is not None:
            temp_dict["section_title"] = self.section_title
        if self._custom_ingestion is not None:
            temp_dict["custom_ingestion"] = self._custom_ingestion

        parameter_dict["_tmp"] = temp_dict

        appearance_dict: Dict[str, Any]
        if isinstance(self.appearance_type, LatchAppearanceEnum):
            appearance_dict = {"type": self.appearance_type.value}
        elif isinstance(self.appearance_type, Multiselect):
            appearance_dict = {"multiselect": asdict(self.appearance_type)}
        else:
            appearance_dict = {}

        if self.placeholder is not None:
            appearance_dict["placeholder"] = self.placeholder
        if self.comment is not None:
            appearance_dict["comment"] = self.comment
        if self.detail is not None:
            appearance_dict["detail"] = self.detail

        appearance_dict["file_type"] = (
            "ANY"
            if self.allow_file and self.allow_dir
            else "FILE"
            if self.allow_file
            else "DIR"
            if self.allow_dir
            else "NONE"
        )

        parameter_dict["appearance"] = appearance_dict

        if len(self.rules) > 0:
            parameter_dict["rules"] = [rule.dict for rule in self.rules]

        return {"__metadata__": parameter_dict}


# https://stackoverflow.com/questions/54668000/type-hint-for-an-instance-of-a-non-specific-dataclass
class _IsDataclass(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Field]]


ParameterType: TypeAlias = Union[
    None,
    int,
    float,
    str,
    bool,
    LatchFile,
    LatchDir,
    Enum,
    _IsDataclass,
    Collection["ParameterType"],
]


T = TypeVar("T", bound=ParameterType)


@dataclass
class SnakemakeParameter(Generic[T], LatchParameter):
    type: Optional[Type[T]] = None
    """
    The python type of the parameter.
    """
    default: Optional[T] = None


@dataclass
class SnakemakeFileParameter(SnakemakeParameter[Union[LatchFile, LatchDir]]):
    """
    Deprecated: use `file_metadata` keyword in `SnakemakeMetadata` instead
    """

    type: Optional[Union[Type[LatchFile], Type[LatchDir]]] = None
    """
    The python type of the parameter.
    """
    path: Optional[Path] = None
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
class NextflowParameter(Generic[T], LatchParameter):
    type: Optional[Type[T]] = None
    """
    The python type of the parameter.
    """
    default: Optional[T] = None
    """
    Default value of the parameter
    """

    samplesheet_type: Literal["csv", "tsv", None] = None
    """
    The type of samplesheet to construct from the input parameter.

    Only used if the provided parameter is a samplesheet (samplesheet=True)
    """
    samplesheet_constructor: Optional[Callable[[T], Path]] = None
    """
    A custom samplesheet constructor.

    Should return the path of the constructed samplesheet. If samplesheet_type is also specified, this takes precedence.
    Only used if the provided parameter is a samplesheet (samplesheet=True)
    """
    results_paths: Optional[List[Path]] = None
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
                default_samplesheet_constructor, t=get_args(self.type)[0], delim=delim
            )
            return

        click.secho(
            dedent(
                """\
            A Samplesheet constructor is required for a samplesheet parameter. Please either provide a value for
            `samplesheet_type` or provide a custom callable to the `samplesheet_constructor` argument.
            """
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1)


DC = TypeVar("DC", bound=_IsDataclass)


def _samplesheet_repr(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, LatchFile) or isinstance(v, LatchDir):
        return v.remote_path
    if isinstance(v, Enum):
        return getattr(v, "value")

    return str(v)


def default_samplesheet_constructor(samples: List[DC], t: DC, delim: str = ",") -> Path:
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

    cpus: Optional[int] = 4
    """
    Number of CPUs required for the task
    """
    memory: Optional[int] = 8
    """
    Memory required for the task in GiB
    """
    storage_gib: Optional[int] = 100
    """
    Storage required for the task in GiB
    """
    storage_expiration_hours: int = 7 * 24
    """
    Number of hours after execution failure that workdir should be retained in EFS/OFS.
    Warning: Increasing this number will increase your Nextflow Storage costs.
    """


@dataclass
class LatchMetadata:
    """Class for organizing workflow metadata

    Example:

    .. code-block:: python

        from latch.types import LatchMetadata, LatchAuthor, LatchRule, LatchAppearanceType

        metadata = LatchMetadata(
            parameters={
                "read1": LatchParameter(
                    display_name="Read 1",
                    description="Paired-end read 1 file to be assembled.",
                    hidden=True,
                    section_title="Sample Reads",
                    placeholder="Select a file",
                    comment="This is a comment",
                    output=False,
                    appearance_type=LatchAppearanceType.paragraph,
                    rules=[
                        LatchRule(
                            regex="(.fasta|.fa|.faa|.fas)$",
                            message="Only .fasta, .fa, .fas, or .faa extensions are valid"
                        )
                    ],
                    batch_table_column=True,  # Show this parameter in batched mode.
                    # The below parameters will be displayed on the side bar of the workflow
                    documentation="https://github.com/author/my_workflow/README.md",
                    author=LatchAuthor(
                        name="Workflow Author",
                        email="licensing@company.com",
                        github="https://github.com/author",
                    ),
                    repository="https://github.com/author/my_workflow",
                    license="MIT",
                    # If the workflow is public, display it under the defined categories on Latch to be more easily discovered by users
                    tags=["NGS", "MAG"],
                ),
        )

        @workflow(metadata)
        def wf(read1: LatchFile):
            ...

    """

    display_name: str
    """The human-readable name of the workflow"""
    author: LatchAuthor
    """ A `LatchAuthor` object that describes the author of the workflow"""
    documentation: Optional[str] = None
    """A link to documentation for the workflow itself"""
    repository: Optional[str] = None
    """A link to the repository where the code for the workflow is hosted"""
    license: str = "MIT"
    """A SPDX identifier"""
    parameters: Dict[str, LatchParameter] = field(default_factory=dict)
    """A dictionary mapping parameter names (strings) to `LatchParameter` objects"""
    wiki_url: Optional[str] = None
    video_tutorial: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    flow: List[FlowBase] = field(default_factory=list)

    no_standard_bulk_execution: bool = False
    """
    Disable the standard CSV-based bulk execution. Intended for workflows that
    support an alternative way of processing bulk data e.g. using a samplesheet
    parameter
    """
    _non_standard: Dict[str, object] = field(default_factory=dict)

    about_page_path: Optional[Path] = None
    """
    Path to a markdown file containing information about the pipeline - rendered in the About page.
    """

    def validate(self):
        if self.about_page_path is not None and not isinstance(
            self.about_page_path, Path
        ):
            click.secho(
                f"`about_page_path` parameter ({self.about_page_path}) must be a"
                " Path object.",
                fg="red",
            )

    @property
    def dict(self):
        metadata_dict = asdict(self)
        # remove parameters since that will be handled by each parameters' dict() method
        del metadata_dict["parameters"]
        metadata_dict["license"] = {"id": self.license}

        # flows override all other rendering, so disable them entirely if not provided
        if len(self.flow) == 0:
            del metadata_dict["flow"]

        for key in self._non_standard:
            metadata_dict[key] = self._non_standard[key]

        return {"__metadata__": metadata_dict}

    def __str__(self):
        def _parameter_str(t: Tuple[str, LatchParameter]):
            parameter_name, parameter_meta = t
            return f"{parameter_name}:\n" + indent(
                str(parameter_meta), "  ", lambda _: True
            )

        metadata_yaml = yaml.safe_dump(self.dict, sort_keys=False)
        parameter_yaml = "".join(map(_parameter_str, self.parameters.items()))
        return (
            metadata_yaml + "Args:\n" + indent(parameter_yaml, "  ", lambda _: True)
        ).strip("\n ")


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
    container_args: List[str] = field(default_factory=list)
    """
    Additional arguments to use when running Docker containers
    """


FileMetadata: TypeAlias = Dict[str, Union[SnakemakeFileMetadata, "FileMetadata"]]


@dataclass
class SnakemakeMetadata(LatchMetadata):
    """Class for organizing Snakemake workflow metadata"""

    output_dir: Optional[LatchDir] = None
    """
    Directory for snakemake workflow outputs
    """
    name: Optional[str] = None
    """
    Name of the workflow
    """
    docker_metadata: Optional[DockerMetadata] = None
    """
    Credentials configuration for private docker repositories
    """
    env_config: EnvironmentConfig = field(default_factory=EnvironmentConfig)
    """
    Environment configuration for spawning Snakemake tasks
    """
    parameters: Dict[str, SnakemakeParameter] = field(default_factory=dict)
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
    about_page_content: Optional[Path] = None
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
                raise click.exceptions.Exit(1)

    def __post_init__(self):
        self.validate()

        if self.name is None:
            self.name = (
                f"snakemake_{identifier_suffix_from_str(self.display_name.lower())}"
            )

        global _snakemake_metadata
        _snakemake_metadata = self

    @property
    def dict(self):
        d = super().dict
        # ayush: Paths aren't JSON serializable but ribosome doesn't need it anyway so we can just delete it
        del d["__metadata__"]["about_page_content"]
        return d


_snakemake_metadata: Optional[SnakemakeMetadata] = None


@dataclass
class NextflowMetadata(LatchMetadata):
    name: Optional[str] = None
    """
    Name of the workflow
    """
    parameters: Dict[str, NextflowParameter] = field(default_factory=dict)
    """
    A dictionary mapping parameter names (strings) to `NextflowParameter` objects
    """
    runtime_resources: NextflowRuntimeResources = field(
        default_factory=NextflowRuntimeResources
    )
    """
    Resources (cpu/memory/storage) for Nextflow runtime task
    """
    execution_profiles: List[str] = field(default_factory=list)
    """
    Execution config profiles to expose to users in the Latch console
    """
    log_dir: Optional[LatchDir] = None
    """
    Directory to dump Nextflow logs
    """
    upload_command_logs: bool = False
    """
    Upload .command.* logs to Latch Data after each task execution
    """

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


_nextflow_metadata: Optional[NextflowMetadata] = None
