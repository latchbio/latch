import typing
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional

import flyteidl.core.interface_pb2 as pb

from ..utils import merged_pb, to_idl_mapping
from .literals import Literal
from .types import LiteralType


@dataclass
class Variable:
    """Defines a strongly typed variable."""

    type: LiteralType
    """Variable literal type."""

    description: str
    """+optional string describing input variable"""

    def to_idl(self) -> pb.Variable:
        return pb.Variable(type=self.type.to_idl(), description=self.description)


@dataclass
class VariableMap:
    """A map of Variables"""

    variables: Mapping[str, Variable]
    """Defines a map of variable names to variables."""

    def to_idl(self) -> pb.VariableMap:
        return pb.VariableMap(variables=to_idl_mapping(self.variables))


@dataclass
class TypedInterface:
    """Defines strongly typed inputs and outputs."""

    inputs: VariableMap
    outputs: VariableMap

    def to_idl(self) -> pb.TypedInterface:
        return pb.TypedInterface(
            inputs=self.inputs.to_idl(), outputs=self.outputs.to_idl()
        )


@dataclass
class Parameter:
    """
    A parameter is used as input to a launch plan and has
    the special ability to have a default value or mark itself as required.
    """

    var: Variable
    """+required Variable. Defines the type of the variable backing this parameter."""

    behavior: "Optional[typing.Union[ParameterBehaviorDefault, ParameterBehaviorRequired]]" = (
        None
    )

    def to_idl(self) -> pb.Parameter:
        return merged_pb(pb.Parameter(var=self.var.to_idl()), self.behavior)


@dataclass
class ParameterBehaviorDefault:
    """Defines a default value that has to match the variable type defined."""

    default: Literal

    def to_idl(self) -> pb.Parameter:
        return pb.Parameter(default=self.default.to_idl())


@dataclass
class ParameterBehaviorRequired:
    """+optional, is this value required to be filled."""

    required: bool

    def to_idl(self) -> pb.Parameter:
        return pb.Parameter(required=self.required)


@dataclass
class ParameterMap:
    """A map of Parameters."""

    parameters: Mapping[str, Parameter]
    """Defines a map of parameter names to parameters."""

    def to_idl(self) -> pb.ParameterMap:
        return pb.ParameterMap(parameters=to_idl_mapping(self.parameters))
