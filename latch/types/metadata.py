import json
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from textwrap import indent
from typing import Any, Dict, List, Optional, Tuple

import yaml


@dataclass
class LatchRule:
    """Class describing a rule that a parameter input must follow

    Fields:
        regex:
            A string regular expression which inputs must match
        message:
            The message to render when an input does not match
            the regex
    """

    regex: str
    message: str

    @property
    def dict(self):
        return asdict(self)

    def __post_init__(self):
        try:
            re.compile(self.regex)
        except re.error as e:
            raise ValueError(f"Malformed regex {self.regex}: {e.msg}")


class LatchAppearanceType(Enum):
    line = "line"
    paragraph = "paragraph"


@dataclass
class LatchAuthor:
    """Class describing metadata about the workflow author

    Fields:
        name:
            The name of the author
        email:
            The email of the author
        github:
            A link to the github profile of the author
    """

    name: Optional[str] = None
    email: Optional[str] = None
    github: Optional[str] = None


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

    ![Example of a user interface for a workflow with a custom flow](../assets/flow-example/flow_example_1.png)

    ![Example of a spoiler flow element](../assets/flow-example/flow_example_spoiler.png)

    The `LatchMedata` for the example above can be defined as follows:

    ```python
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
    ```

    Args:
        section:
            Title of the section

        flow:
            Flow displayed in the section card
    """

    section: str
    flow: List[FlowBase]

    def __init__(self, section: str, *flow: FlowBase):
        object.__setattr__(self, "section", section)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True)
class Text(FlowBase):
    """Flow element that displays a markdown string

    Args:
        text:
            Markdown body text
    """

    text: str


@dataclass(frozen=True)
class Title(FlowBase):
    """Flow element that displays a markdown title

    Args:
        title:
            Markdown title text
    """

    title: str


@dataclass(frozen=True, init=False)
class Params(FlowBase):
    """Flow element that displays parameter widgets

    Args:
        params:
            Names of parameters whose widgets will be displayed.
            Order is preserved. Duplicates are allowed
    """

    params: List[str]

    def __init__(self, *args: str):
        object.__setattr__(self, "params", list(args))


@dataclass(frozen=True, init=False)
class Spoiler(FlowBase):
    """Flow element that displays a collapsible card with a given title

    Args:
        spoiler:
            Title of the spoiler

        flow:
            Flow displayed in the spoiler card
    """

    spoiler: str
    flow: List[FlowBase]

    def __init__(self, spoiler: str, *flow: FlowBase):
        object.__setattr__(self, "spoiler", spoiler)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class ForkBranch:
    """Definition of a :class:`~latch.types.metadata.Fork` branch

    Args:
        display_name:
            String displayed in the fork's multibutton

        flow:
            Child flow displayed in the fork card when the branch is active
    """

    display_name: str
    flow: List[FlowBase]

    def __init__(self, display_name: str, *flow: FlowBase):
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class Fork(FlowBase):
    """Flow element that displays a set of mutually exclusive alternatives

    Displays a title, followed by a horizontal multibutton for selecting a branch,
    then a card for the active branch

    Args:
        fork:
            Name of a `str`-typed parameter to store the active branch's key

        display_name:
            Title shown above the fork selector

        flows:
            Mapping between branch keys to branch definitions.
            Order determines the order of options in the multibutton
    """

    fork: str
    display_name: str
    flows: Dict[str, ForkBranch]

    def __init__(self, fork: str, display_name: str, **flows: ForkBranch):
        object.__setattr__(self, "fork", fork)
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flows", flows)


@dataclass
class LatchParameter:
    """Class for organizing parameter metadata

    Fields:
        display_name:
            The name used to display the parameter on Latch Console
        description:
            The description of the parameter's role in the workflow
        hidden:
            Whether or not the parameter should be hidden by default
        section_title:
            Whether this parameter should start a new section
        placeholder:
            What should be rendered as a placeholder in the input box
            of the parameter before any value is inputed.
        comment:
            Any comment on the parameter itself
        output:
            Whether or not this parameter is an output (used to disable
            path validation before launching a workflow)
        batch_table_column:
            Whether this parameter should be given a column in the batch
            table at the top of the workflow inputs
        appearance_type:
            Whether the parameter should be rendered as a line or paragraph
            (must be exactly one of either LatchAppearanceType.line or
            LatchAppearanceType.paragraph)
        rules:
            A list of LatchRule objects that inputs to this parameter must
            follow.
        samplesheet:
            Use samplesheet input UI. Allows importing from Latch Registry.
            Parameter type must be a list of dataclasses
    """

    display_name: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    section_title: Optional[str] = None
    placeholder: Optional[str] = None
    comment: Optional[str] = None
    output: bool = False
    batch_table_column: bool = False
    appearance_type: LatchAppearanceType = LatchAppearanceType.line
    rules: List[LatchRule] = field(default_factory=list)
    detail: Optional[str] = None
    samplesheet: Optional[bool] = None
    _custom_ingestion: Optional[str] = None

    def __str__(self):
        metadata_yaml = yaml.safe_dump(self.dict, sort_keys=False)
        if self.description is not None:
            return f"{self.description}\n{metadata_yaml}"
        return metadata_yaml

    @property
    def dict(self):
        parameter_dict = {"display_name": self.display_name}
        if self.output:
            parameter_dict["output"] = True
        if self.batch_table_column:
            parameter_dict["batch_table_column"] = True
        if self.samplesheet:
            parameter_dict["samplesheet"] = True

        temp_dict = {"hidden": self.hidden}
        if self.section_title is not None:
            temp_dict["section_title"] = self.section_title
        if self._custom_ingestion is not None:
            temp_dict["custom_ingestion"] = self._custom_ingestion

        parameter_dict["_tmp"] = temp_dict

        appearance_dict = {"type": self.appearance_type.value}
        if self.placeholder is not None:
            appearance_dict["placeholder"] = self.placeholder
        if self.comment is not None:
            appearance_dict["comment"] = self.comment
        if self.detail is not None:
            appearance_dict["detail"] = self.detail
        parameter_dict["appearance"] = appearance_dict

        if len(self.rules) > 0:
            rules = []
            for rule in self.rules:
                rules.append(rule.dict)
            parameter_dict["rules"] = rules

        return {"__metadata__": parameter_dict}


@dataclass
class LatchMetadata:
    """Class for organizing workflow metadata

    Example:

    ```python
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
    ```


    Fields:
        display_name:
            The name of the workflow
        author:
            A `LatchAuthor` object that describes the author of the workflow
        documentation:
            A link to documentation for the workflow itself
        repository:
            A link to the repository where the code for the workflow is hosted
        license:
            A SPDX identifier
        parameters:
            A dictionary mapping parameter names (strings) to `LatchParameter` objects
        no_standard_bulk_execution:
            Disable the standard CSV-based bulk execution. Intended for workflows that
            support an aleternative way of processing bulk data e.g. using a samplesheet
            parameter
    """

    display_name: str
    author: LatchAuthor
    documentation: Optional[str] = None
    repository: Optional[str] = None
    license: str = "MIT"
    parameters: Dict[str, LatchParameter] = field(default_factory=dict)
    wiki_url: Optional[str] = None
    video_tutorial: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    flow: List[FlowBase] = field(default_factory=list)

    no_standard_bulk_execution: bool = False
    _non_standard: Dict[str, object] = field(default_factory=dict)

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
