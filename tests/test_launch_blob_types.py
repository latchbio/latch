# Flyte does not ship type stubs; retain the remaining strict type checks.
# pyright: reportMissingTypeStubs=false
from typing import Union

import pytest
from flytekit.core.type_engine import TypeEngine
from flytekit.models.core.types import BlobType
from flytekit.models.interface import Variable
from flytekit.models.types import LiteralType

from latch.ldata.path import LPath
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.services.launch.type_converter import convert_inputs_to_literals


@pytest.mark.parametrize("value_type", [LatchFile, LatchDir])
def test_blob_union_selects_typed_variant(value_type: type[Union[LatchFile, LatchDir]]):
    value = value_type("latch://1.account/input")
    union_type = TypeEngine.to_literal_type(Union[LatchFile, LatchDir])  # pyright: ignore[reportArgumentType]
    result = convert_inputs_to_literals(
        {"input": value}, {"input": Variable(union_type, "")}
    )["input"]
    expected = (
        BlobType.BlobDimensionality.SINGLE
        if value_type is LatchFile
        else BlobType.BlobDimensionality.MULTIPART
    )
    assert result.scalar is not None
    union = result.scalar.union
    assert union.stored_type.blob.dimensionality == expected
    assert union.value.scalar.blob.uri == "latch://1.account/input"


@pytest.mark.parametrize(
    ("value_type", "dimensionality"),
    [
        (LatchFile, BlobType.BlobDimensionality.MULTIPART),
        (LatchDir, BlobType.BlobDimensionality.SINGLE),
    ],
)
def test_blob_rejects_incompatible_typed_value(
    value_type: type[Union[LatchFile, LatchDir]], dimensionality: int
):
    value = value_type("latch://1.account/input")
    literal_type = LiteralType(blob=BlobType(format="", dimensionality=dimensionality))
    with pytest.raises(TypeError, match="dimensionality"):
        convert_inputs_to_literals(
            {"input": value}, {"input": Variable(literal_type, "")}
        )


@pytest.mark.parametrize(
    "dimensionality",
    [BlobType.BlobDimensionality.SINGLE, BlobType.BlobDimensionality.MULTIPART],
)
@pytest.mark.parametrize(
    "value", ["latch://1.account/input", LPath("latch://1.account/input")]
)
def test_untyped_uri_keeps_explicit_blob_type(value: object, dimensionality: int):
    blob_type = BlobType(format="", dimensionality=dimensionality)
    literal_type = LiteralType(blob=blob_type)
    result = convert_inputs_to_literals(
        {"input": value}, {"input": Variable(literal_type, "")}
    )["input"]
    assert result.scalar is not None
    assert result.scalar.blob.metadata.type.dimensionality == dimensionality
    assert result.scalar.blob.uri == "latch://1.account/input"
