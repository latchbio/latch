from typing import Dict, List, Union

import graphql.language as l
import graphql.language.parser as lp
from typing_extensions import TypeAlias


def _parse_selection(x: str) -> l.SelectionNode:
    p = lp.Parser(l.Source(x.lstrip()))
    p.expect_token(l.TokenKind.SOF)
    res = p.parse_selection()
    p.expect_token(l.TokenKind.EOF)
    return res


def _var_def_node(x: str, typ: l.TypeNode) -> l.VariableDefinitionNode:
    res = l.VariableDefinitionNode()
    res.variable = _var_node(x)
    res.type = typ
    return res


def _var_node(x: str) -> l.VariableNode:
    res = l.VariableNode()
    res.name = _name_node(x)
    return res


def _name_node(x: str) -> l.NameNode:
    res = l.NameNode()
    res.value = x
    return res


_GqlJsonArray: TypeAlias = List["_GqlJsonValue"]
_GqlJsonObject: TypeAlias = Dict[str, "_GqlJsonValue"]
_GqlJsonValue: TypeAlias = Union[
    _GqlJsonObject, _GqlJsonArray, str, int, float, bool, None, l.Node
]


def _obj_field(k: str, x: _GqlJsonValue) -> l.ObjectFieldNode:
    res = l.ObjectFieldNode()

    res.name = _name_node(k)
    res.value = _json_value(x)

    return res


def _json_value(x: _GqlJsonValue) -> l.ValueNode:
    # note: this does not support enums

    if isinstance(x, l.Node):
        return x

    if x is None:
        return l.NullValueNode()

    if isinstance(x, str):
        res = l.StringValueNode()
        res.value = x
        return res

    if isinstance(x, int):
        if isinstance(x, bool):
            res = l.BooleanValueNode()
            res.value = x
            return res

        res = l.IntValueNode()
        res.value = str(x)
        return res

    if isinstance(x, float):
        res = l.FloatValueNode()
        res.value = str(x)
        return res

    if isinstance(x, float):
        res = l.FloatValueNode()
        res.value = str(x)
        return res

    if isinstance(x, list):
        res = l.ListValueNode()
        res.values = tuple(_json_value(el) for el in x)
        return res

    if isinstance(x, dict):
        res = l.ObjectValueNode()
        res.fields = tuple(_obj_field(k, v) for k, v in x.items())
        return res

    raise ValueError(f"cannot Graphql-serialize JSON value of type {type(x)}: {x}")
