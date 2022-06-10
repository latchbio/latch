from latch.types.file import LatchFile
from enum import Enum
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import List, Union, Annotated
from flytekit.core.with_metadata import FlyteMetadata

@dataclass_json
@dataclass
class SingleEndReads:
    r1: LatchFile


@dataclass_json
@dataclass
class PairedEndReads:
    r1: LatchFile
    r2: LatchFile


class Strandedness(Enum):
    auto: Annotated(str, FlyteMetadata({"display_name": "Auto-Detect"})) = "auto"

@dataclass_json
@dataclass
class SampleSheet:
    name: str
    strandedness: Strandedness
    replicates: List[Union[SingleEndReads, PairedEndReads]]
