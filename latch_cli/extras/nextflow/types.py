from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple, TypeVar, Union

T = TypeVar("T")


class VertexType(Enum):
    process = "process"
    operator = "operator"
    origin = "origin"


class NextflowInputParamType(Enum):
    default = "default"
    val = "val"
    tuple = "tuple"
    path = "path"


class NextflowOutputParamType(Enum):
    stdoutparam = "stdoutparam"
    valueoutparam = "valueoutparam"
    tupleoutparam = "tupleoutparam"
    fileoutparam = "fileoutparam"
    envoutparam = "envoutparam"


NextflowParamType = Union[NextflowInputParamType, NextflowOutputParamType]


@dataclass
class NextflowParam:
    name: str
    type: NextflowParamType


@dataclass
class NextflowDAGVertex:
    id: int
    label: Optional[str]  # todo(ayush): do these need to be optional
    vertex_type: VertexType
    input_params: Optional[List[NextflowParam]]  # empty list zero state instead
    output_params: Optional[List[NextflowParam]]
    code: Optional[str]


@dataclass
class NextflowDAGEdge:
    id: int
    to_idx: Optional[int]
    from_idx: Optional[int]
    label: Optional[str]
    connection: Tuple[int, int]
