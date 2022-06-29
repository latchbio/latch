import json
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from textwrap import indent
from typing import Any, List, Optional, Tuple

import yaml


@dataclass
class LatchRule:
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
    name: Optional[str] = None
    email: Optional[str] = None
    github: Optional[str] = None


@dataclass
class LatchParameter:
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

    def __str__(self):
        metadata_yaml = yaml.dump(yaml.safe_load(json.dumps(self.dict)))
        return f"{self.description}\n{metadata_yaml}"

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
        parameter_dict["_tmp"] = temp_dict

        appearance_dict = {"type": self.appearance_type.value}
        if self.placeholder is not None:
            appearance_dict["placeholder"] = self.placeholder
        if self.comment is not None:
            appearance_dict["comment"] = self.comment
        parameter_dict["appearance"] = appearance_dict

        if len(self.rules) > 0:
            rules = []
            for rule in self.rules:
                rules.append(rule.dict)
            parameter_dict["rules"] = rules

        return {"__metadata__": parameter_dict}


@dataclass
class LatchMetadata:
    display_name: str
    author: LatchAuthor
    documentation: Optional[str] = None
    repository: Optional[str] = None
    license: str = "MIT"
    parameters: dict[str, LatchParameter] = field(default_factory=dict)

    @property
    def dict(self):
        metadata_dict = asdict(self)
        metadata_dict["license"] = {"id": self.license}
        return {"__metadata__": metadata_dict}

    def __str__(self):
        def _parameter_str(t: Tuple[str, LatchParameter]):
            parameter_name, parameter_meta = t
            return f"{parameter_name}:\n" + indent(
                str(parameter_meta), "  ", lambda _: True
            )

        metadata_yaml = yaml.dump(yaml.safe_load(json.dumps(self.dict)))
        parameter_yaml = "\n".join(map(_parameter_str, self.parameters.items()))
        return (
            metadata_yaml + "Args:\n" + indent(parameter_yaml, "  ", lambda _: True)
        ).strip("\n ")
