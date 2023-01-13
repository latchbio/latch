import types
from dataclasses import dataclass, make_dataclass
from enum import Enum
from typing import List, T, Tuple, Union

from dataclasses_json import dataclass_json

from latch.type_engine import best_effort_python_val, build_python_literal


def make_dataclass_json(cls_name: str, fields: List[Tuple[str, T]]):
    annotations = {}
    for name, tp in fields:
        annotations[name] = tp

    def exec_body_callback(ns):
        ns["__annotations__"] = annotations

    cls = types.new_class(cls_name, exec_body=exec_body_callback)
    return dataclass_json(cls)


primitives = [
    int,
    str,
    float,
    bool,
    type(None),
]

lists = []
for x in primitives:
    lists.append(List[x])

unions = {}
for x in primitives:
    for y in primitives:
        candidate = Union[(x, y)]
        if candidate not in unions:
            unions[candidate] = True
unions = list(unions.keys())


class test_enum(Enum):
    a = "a"
    b = "b"


test_enum._name = "test_enum"  # injected by type transformer
test_enum._variants = ["a", "b"]  # injected by type transformer


fields = []
field_name = 97  # a
for x in primitives:
    fields.append((chr(field_name), x))
    field_name += 1
simple_dataclass = make_dataclass("simple_dataclass", fields)
simple_dataclass_json = dataclass_json(make_dataclass("simple_dataclass", fields))


fields = []
field_name = 97  # a
for x in lists + unions + [test_enum]:
    fields.append((chr(field_name), x))
    field_name += 1
complex_dataclass = make_dataclass("complex_dataclass", fields)
complex_dataclass_json = dataclass_json(make_dataclass("complex_dataclass", fields))

fields = [("a", simple_dataclass), ("b", complex_dataclass)]
nested_dataclass = make_dataclass("nested_dataclass", fields)
nested_dataclass_json = dataclass_json(make_dataclass("nested_dataclass", fields))

simple_dataclass_val = simple_dataclass(a=0, b="foo", c=0.0, d=False, e=None)
assert best_effort_python_val(simple_dataclass) == simple_dataclass_val

simple_dataclass_json_val = simple_dataclass_json(a=0, b="foo", c=0.0, d=False, e=None)
assert best_effort_python_val(simple_dataclass_json) == simple_dataclass_json_val


complex_dataclass_val = complex_dataclass(
    a=[0],
    b=["foo"],
    c=[0.0],
    d=[False],
    e=[None],
    f=0,
    g=0,
    h=0,
    i=0,
    j=0,
    k="foo",
    l="foo",
    m="foo",
    n="foo",
    o=0.0,
    p=0.0,
    q=0.0,
    r=False,
    s=False,
    t=None,
    u=test_enum.a,
)
assert best_effort_python_val(complex_dataclass) == complex_dataclass_val


complex_dataclass_json_val = complex_dataclass_json(
    a=[0],
    b=["foo"],
    c=[0.0],
    d=[False],
    e=[None],
    f=0,
    g=0,
    h=0,
    i=0,
    j=0,
    k="foo",
    l="foo",
    m="foo",
    n="foo",
    o=0.0,
    p=0.0,
    q=0.0,
    r=False,
    s=False,
    t=None,
    u=test_enum.a,
)
assert best_effort_python_val(complex_dataclass_json) == complex_dataclass_json_val


assert best_effort_python_val(nested_dataclass) == nested_dataclass(
    a=simple_dataclass_val, b=complex_dataclass_val
)


best_effort = best_effort_python_val(nested_dataclass_json)
test_val = nested_dataclass_json(
    a=simple_dataclass_json_val, b=complex_dataclass_json_val
)

# TODO - vals seem same but equality test fails
# best_effort = best_effort_python_val(nested_dataclass)
# test_val = nested_dataclass(a=simple_dataclass_val, b=complex_dataclass_val)
