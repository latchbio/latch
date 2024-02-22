import typing
from dataclasses import dataclass
from enum import Enum

import flyteidl.core.condition_pb2 as pb

from .literals import Primitive


@dataclass
class ComparsionExpression:
    """
    Defines a 2-level tree where the root is a comparison operator and Operands are primitives or known variables.
    Each expression results in a boolean result.
    """

    class Operator(int, Enum):
        """Binary Operator for each expression"""

        eq = pb.ComparisonExpression.EQ
        neq = pb.ComparisonExpression.NEQ
        # Greater Than
        gt = pb.ComparisonExpression.GT
        gte = pb.ComparisonExpression.GTE
        # Less Than
        lt = pb.ComparisonExpression.LT
        lte = pb.ComparisonExpression.LTE

        def to_idl(self) -> pb.ComparisonExpression.Operator:
            return self.value

    operator: Operator
    left_value: "Operand"
    right_value: "Operand"

    def to_idl(self) -> pb.ComparisonExpression:
        return pb.ComparisonExpression(
            operator=self.operator.to_idl(),
            left_value=self.left_value.to_idl(),
            right_value=self.right_value.to_idl(),
        )


@dataclass
class Operand:
    """Defines an operand to a comparison expression."""

    val: "typing.Union[OperandPrimitive, OperandVar]"

    def to_idl(self) -> pb.Operand:
        return self.val.to_idl()


@dataclass
class OperandPrimitive:
    primitive: Primitive
    """Can be a constant"""

    def to_idl(self) -> pb.Operand:
        return pb.Operand(primitive=self.primitive.to_idl())


@dataclass
class OperandVar:
    var: str
    """Or one of this node's input variables"""

    def to_idl(self) -> pb.Operand:
        return pb.Operand(var=self.var)


@dataclass
class BooleanExpression:
    """
    Defines a boolean expression tree. It can be a simple or a conjunction expression.
    Multiple expressions can be combined using a conjunction or a disjunction to result in a final boolean result.
    """

    expr: "typing.Union[BooleanExpressionConjuctionExpression, BooleanExpressionComparisonExpression]"

    def to_idl(self) -> pb.BooleanExpression:
        return self.expr.to_idl()


@dataclass
class BooleanExpressionConjuctionExpression:
    conjunction_expression: "ConjuctionExpression"

    def to_idl(self) -> pb.BooleanExpression:
        return pb.BooleanExpression(conjunction=self.conjunction_expression.to_idl())


@dataclass
class BooleanExpressionComparisonExpression:
    comparison_expression: ComparsionExpression

    def to_idl(self) -> pb.BooleanExpression:
        return pb.BooleanExpression(comparison=self.comparison_expression.to_idl())


@dataclass
class ConjuctionExpression:
    """Defines a conjunction expression of two boolean expressions."""

    class LogicalOperator(int, Enum):
        """Nested conditions. They can be conjoined using AND / OR"""

        # Conjunction
        and_ = pb.ConjunctionExpression.AND
        or_ = pb.ConjunctionExpression.OR

        def to_idl(self) -> pb.ConjunctionExpression.LogicalOperator:
            return self.value

    operator: LogicalOperator
    left_expression: BooleanExpression
    right_expression: BooleanExpression

    def to_idl(self) -> pb.ConjunctionExpression:
        return pb.ConjunctionExpression(
            operator=self.operator.to_idl(),
            left_expression=self.left_expression.to_idl(),
            right_expression=self.right_expression.to_idl(),
        )
