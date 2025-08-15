from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Generic, Literal, Optional, TypeVar, Union

from typing_extensions import Self

from latch.types.metadata import (
    FlowBase,
    LatchAppearance,
    LatchAppearanceEnum,
    LatchRule,
)
from latch_cli.services.register.utils import import_module_by_path

T = TypeVar("T")


class Default(Enum):
    empty = 1


@dataclass
class ParameterMeta(Generic[T]):
    display_name: str
    type: type[T]

    default: Union[T, Literal[Default.empty]] = Default.empty

    # todo(ayush): mostly copied over from LatchParameter/NextflowParameter - clean up +
    description: Optional[str] = None
    hidden: bool = False
    section_title: Optional[str] = None
    placeholder: Optional[str] = None
    comment: Optional[str] = None
    output: bool = False
    batch_table_column: bool = False
    allow_dir: bool = True
    allow_file: bool = True
    appearance_type: LatchAppearance = LatchAppearanceEnum.line
    rules: list[LatchRule] = field(default_factory=list)
    detail: Optional[str] = None
    samplesheet: Optional[bool] = None
    allowed_tables: Optional[list[int]] = None

    samplesheet_type: Literal["csv", "tsv"] = "csv"
    samplesheet_constructor: Optional[Callable[[T], Path]] = None
    results_paths: Optional[list[Path]] = None


class NextflowMetadataBuilder:
    def __init__(self):
        self.parameters: dict[str, ParameterMeta[object]] = {}
        self.flow: list[FlowBase] = []

    @classmethod
    def from_file(cls, file: Path) -> Self:
        latch_metadata = import_module_by_path(file)

        args = getattr(latch_metadata, "WorkflowArgs", None)
        flow = getattr(latch_metadata, "flow", None)

        return cls()


NextflowMetadataBuilder.from_file(
    Path("/Users/ayush/Desktop/workflows/theiagen-demo/latch_metadata/__init__.py")
)
