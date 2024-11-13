from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from textwrap import indent
from typing import TYPE_CHECKING, Any, Union

import yaml
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from .flows import FlowBase


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
            raise ValueError(f"Malformed regex {self.regex}: {e.msg}") from e


class LatchAppearanceEnum(Enum):
    line = "line"
    paragraph = "paragraph"


@dataclass(frozen=True)
class MultiselectOption:
    name: str
    value: object


@dataclass(frozen=True)
class Multiselect:
    options: list[MultiselectOption] = field(default_factory=list)
    allow_custom: bool = False


# backwards compatibility
LatchAppearanceType = LatchAppearanceEnum

LatchAppearance: TypeAlias = Union[LatchAppearanceEnum, Multiselect]


@dataclass
class LatchAuthor:
    """Class describing metadata about the workflow author"""

    name: str | None = None
    """The name of the author"""
    email: str | None = None
    """The email of the author"""
    github: str | None = None
    """A link to the github profile of the author"""


@dataclass
class LatchParameter:
    """Class for organizing parameter metadata"""

    display_name: str | None = None
    """The name used to display the parameter on Latch Console"""
    description: str | None = None
    """The description of the parameter's role in the workflow"""
    hidden: bool = False
    """Whether or not the parameter should be hidden by default"""
    section_title: str | None = None
    """Whether this parameter should start a new section"""
    placeholder: str | None = None
    """
    What should be rendered as a placeholder in the input box
    of the parameter before any value is inputed.
    """
    comment: str | None = None
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
    rules: list[LatchRule] = field(default_factory=list)
    """
    A list of LatchRule objects that inputs to this parameter must follow
    """
    detail: str | None = None
    samplesheet: bool | None = None
    """
    Use samplesheet input UI. Allows importing from Latch Registry.
    Parameter type must be a list of dataclasses
    """
    allowed_tables: list[int] | None = None
    """
    If using the samplesheet component, specify a set of Registry Tables (by ID) to allow selection from.
    If not provided, all Tables are allowed.

    Only has an effect if `samplesheet=True`.
    """
    _custom_ingestion: str | None = None

    def __str__(self):
        metadata_yaml = yaml.safe_dump(self.dict, sort_keys=False)
        if self.description is not None:
            return f"{self.description}\n{metadata_yaml}"
        return metadata_yaml

    @property
    def dict(self):
        parameter_dict: dict[str, Any] = {"display_name": self.display_name}

        if self.output:
            parameter_dict["output"] = True
        if self.batch_table_column:
            parameter_dict["batch_table_column"] = True
        if self.samplesheet:
            parameter_dict["samplesheet"] = True
            if self.allowed_tables is not None:
                parameter_dict["allowed_tables"] = [str(x) for x in self.allowed_tables]

        temp_dict: dict[str, Any] = {"hidden": self.hidden}
        if self.section_title is not None:
            temp_dict["section_title"] = self.section_title
        if self._custom_ingestion is not None:
            temp_dict["custom_ingestion"] = self._custom_ingestion

        parameter_dict["_tmp"] = temp_dict

        appearance_dict: dict[str, Any]
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
    documentation: str | None = None
    """A link to documentation for the workflow itself"""
    repository: str | None = None
    """A link to the repository where the code for the workflow is hosted"""
    license: str = "MIT"
    """A SPDX identifier"""
    parameters: dict[str, LatchParameter] = field(default_factory=dict)
    """A dictionary mapping parameter names (strings) to `LatchParameter` objects"""
    wiki_url: str | None = None
    video_tutorial: str | None = None
    tags: list[str] = field(default_factory=list)
    flow: list[FlowBase] = field(default_factory=list)

    no_standard_bulk_execution: bool = False
    """
    Disable the standard CSV-based bulk execution. Intended for workflows that
    support an alternative way of processing bulk data e.g. using a samplesheet
    parameter
    """
    _non_standard: dict[str, object] = field(default_factory=dict)

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
        def _parameter_str(t: tuple[str, LatchParameter]):
            parameter_name, parameter_meta = t
            return f"{parameter_name}:\n" + indent(
                str(parameter_meta), "  ", lambda _: True
            )

        metadata_yaml = yaml.safe_dump(self.dict, sort_keys=False)
        parameter_yaml = "".join(map(_parameter_str, self.parameters.items()))
        return (
            metadata_yaml + "Args:\n" + indent(parameter_yaml, "  ", lambda _: True)
        ).strip("\n ")
