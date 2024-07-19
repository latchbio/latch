import inspect
import sys
from dataclasses import dataclass
from typing import List
from typing import Any
from typing import Collection, Iterable, Optional, Union, Mapping, Dict, Set, Tuple

import pytest

from latch.resources.workflow import _is_list_of_dataclasses_type
from latch.resources.workflow import _is_valid_samplesheet_parameter_type
from latch.resources.workflow import _is_optional_type
from latch.resources.workflow import _is_type_annotation
from latch.resources.workflow import _unpack_optional_type
from latch.resources.workflow import TypeAnnotation

PRIMITIVE_TYPES: List[type] = [int, float, bool, str]
COLLECTION_TYPES: List[TypeAnnotation] = [List[int], Dict[str, int], Set[int], Tuple[int], Mapping[str, int], Iterable[int], Collection[int]]

if sys.version_info >= (3, 10):
    COLLECTION_TYPES += [list[int], dict[str, int], set[int], tuple[int]]

OPTIONAL_TYPES: List[TypeAnnotation] = [Optional[T] for T in (PRIMITIVE_TYPES + COLLECTION_TYPES)]
OPTIONAL_TYPES += [Union[T, None] for T in (PRIMITIVE_TYPES + COLLECTION_TYPES)]

if sys.version_info >= (3, 10):
    OPTIONAL_TYPES += [T | None for T in (PRIMITIVE_TYPES + COLLECTION_TYPES)]


@dataclass
class FakeDataclass:
    """A dataclass for testing."""
    foo: str
    bar: int


# Enumerate the possible ways to declare a list or optional list of dataclasses
SAMPLESHEET_TYPES: List[TypeAnnotation] = [
    List[FakeDataclass],
    Optional[List[FakeDataclass]],
    Union[List[FakeDataclass], None],
]

if sys.version_info >= (3, 10):
    SAMPLESHEET_TYPES += [
        list[FakeDataclass],
        Optional[list[FakeDataclass]],
        Union[list[FakeDataclass], None],
        list[FakeDataclass] | None,
        List[FakeDataclass] | None,
    ]


@pytest.mark.parametrize("dtype", SAMPLESHEET_TYPES)
def test_is_valid_samplesheet_parameter_type(dtype: TypeAnnotation) -> None:
    """
    `_is_valid_samplesheet_parameter_type` should accept a type that is a list of dataclasses, or an
    `Optional` list of dataclasses.
    """
    parameter = inspect.Parameter("foo", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=dtype)
    assert _is_valid_samplesheet_parameter_type(parameter) is True


def test_is_list_of_dataclasses_type() -> None:
    """
    `_is_list_of_dataclasses_type` should accept a type that is a list of dataclasses.
    """
    assert _is_list_of_dataclasses_type(List[FakeDataclass]) is True


@pytest.mark.parametrize("bad_type", [
    str,  # Not a list
    int,  # Not a list
    List[str],  # Not a list of dataclasses
    List[int],  # Not a list of dataclasses
    FakeDataclass,  # Not a list
])
def test_is_list_of_dataclasses_type_rejects_bad_type(bad_type: type) -> None:
    """
    `_is_list_of_dataclasses_type` should reject anything else.
    """
    assert _is_list_of_dataclasses_type(bad_type) is False


def test_is_list_of_dataclasses_type_raises_if_not_a_type() -> None:
    """
    `is_list_of_dataclasses_type` should raise a `TypeError` if the input is not a type.
    """
    with pytest.raises(TypeError):
        _is_list_of_dataclasses_type([FakeDataclass("hello", 1)])


@pytest.mark.parametrize("dtype", OPTIONAL_TYPES)
def test_is_optional_type(dtype: TypeAnnotation) -> None:
    """`_is_optional_type` should return True for `Optional[T]` types."""
    assert _is_optional_type(dtype) is True


@pytest.mark.parametrize("dtype", PRIMITIVE_TYPES + COLLECTION_TYPES)
def test_is_optional_type_returns_false_if_not_optional(dtype: TypeAnnotation) -> None:
    """`_is_optional_type` should return False for non-Optional types."""
    assert _is_optional_type(dtype) is False


@pytest.mark.parametrize("dtype", PRIMITIVE_TYPES + COLLECTION_TYPES)
def test_unpack_optional_type(dtype: TypeAnnotation) -> None:
    """`_unpack_optional_type()` should return the base type of `Optional[T]` types."""
    assert _unpack_optional_type(Optional[dtype]) is dtype
    assert _unpack_optional_type(Union[dtype, None]) is dtype
    if sys.version_info >= (3, 10):
        assert _unpack_optional_type(dtype | None) is dtype



@pytest.mark.parametrize("annotation", PRIMITIVE_TYPES + COLLECTION_TYPES + OPTIONAL_TYPES)
def test_is_type_annotation(annotation: TypeAnnotation) -> None:
    """
    `_is_type_annotation()` should return True for any valid type annotation.
    """
    assert _is_type_annotation(annotation) is True


def test_is_type_annotation_returns_false_if_empty() -> None:
    """
    `_is_type_annotation()` should only return False if the annotation is `Parameter.empty`.
    """
    assert _is_type_annotation(inspect.Parameter.empty) is False


@pytest.mark.parametrize("bad_annotation", [1, "abc", [1, 2], {"foo": 1}, FakeDataclass("hello", 1)])
def test_is_type_annotation_raises_if_annotation_is_not_a_type(bad_annotation: Any) -> None:
    """
    `_is_type_annotation()` should raise `TypeError` for any non-type object.
    """
    with pytest.raises(TypeError):
        _is_type_annotation(bad_annotation)