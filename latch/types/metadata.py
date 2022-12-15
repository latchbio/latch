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

    Args:
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

    Args:
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
    ...


@dataclass(frozen=True, init=False)
class Section(FlowBase):
    section: str
    flow: List[FlowBase]

    def __init__(self, section: str, *flow: FlowBase):
        object.__setattr__(self, "section", section)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True)
class Text(FlowBase):
    text: str


@dataclass(frozen=True)
class Title(FlowBase):
    title: str


@dataclass(frozen=True, init=False)
class Params(FlowBase):
    params: List[str]

    def __init__(self, *args: str):
        object.__setattr__(self, "params", list(args))


@dataclass(frozen=True, init=False)
class Spoiler(FlowBase):
    spoiler: str
    flow: List[FlowBase]

    def __init__(self, spoiler: str, *flow: FlowBase):
        object.__setattr__(self, "spoiler", spoiler)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class ForkBranch:
    display_name: str
    flow: List[FlowBase]

    def __init__(self, display_name: str, *flow: FlowBase):
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class Fork(FlowBase):
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

    Args:
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
    _custom_ingestion: Optional[str] = None

    def __str__(self):
        metadata_yaml = yaml.safe_dump(self.dict)
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
    """Workflow information container for use in the :meth:`@workflow <latch.resources.workflow>` decorator

    Metadata is visible both in the workflow sidebar and on the workflow card

    .. image:: /assets/LatchMetadata/explore.png
        :alt: Workflow information UI

    Args:
        display_name:
            Human-readable name

            .. image:: /assets/LatchMetadata/display_name.png
                :alt: Workflow display name UI
        author:
            Information about the workflow author

            .. image:: /assets/LatchMetadata/author.png
                :alt: Workflow author UI
        documentation:
            Link to a documentation page for the workflow

            .. image:: /assets/LatchMetadata/documentation.png
                :alt: Workflow documentation link UI
        repository:
            Link to the workflow source code repository

            .. image:: /assets/LatchMetadata/github.png
                :alt: Workflow source code repository link UI
        license:
            `SPDX identifier <https://spdx.org/licenses/>`_ of the workflow source code license

            .. image:: /assets/LatchMetadata/license.png
                :alt: Workflow source code license UI
        parameters:
            Map from parameter names to :class:`metadata <LatchParameter>`
        video_tutorial:
            Link to a video showcase of the workflow

            .. image:: /assets/LatchMetadata/video.png
                :alt: Workflow video tutorial link UI
        tags:
            Keywords for workflow search, currently only allows a predefined set of biological domains

            .. image:: /assets/LatchMetadata/tags.png
                :alt: Workflow tags UI
        flow:
            Custom `parameter flow declaration <basics/workflow_ui.html#custom-parameter-flow>`_
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

    @property
    def dict(self):
        metadata_dict = asdict(self)
        # remove parameters since that will be handled by each parameters' dict() method
        del metadata_dict["parameters"]
        metadata_dict["license"] = {"id": self.license}

        # flows override all other rendering, so disable them entirely if not provided
        if len(self.flow) == 0:
            del metadata_dict["flow"]

        return {"__metadata__": metadata_dict}

    def __str__(self):
        def _parameter_str(t: Tuple[str, LatchParameter]):
            parameter_name, parameter_meta = t
            return f"{parameter_name}:\n" + indent(
                str(parameter_meta), "  ", lambda _: True
            )

        metadata_yaml = yaml.safe_dump(self.dict)
        parameter_yaml = "".join(map(_parameter_str, self.parameters.items()))
        return (
            metadata_yaml + "Args:\n" + indent(parameter_yaml, "  ", lambda _: True)
        ).strip("\n ")
