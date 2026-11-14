import json
from pathlib import Path

import pytest

from latch_cli.nextflow.config import get_python_type_inner
from latch_cli.nextflow.parse_schema import parse_schema


@pytest.mark.parametrize(
    ("spec", "default"),
    [
        ({"type": "integer", "enum": [33, 64], "default": 33}, "33"),
        ({"type": "number", "enum": [0, 1, 2], "default": 2}, "2"),
        ({"type": "string", "enum": ["a", "b"], "default": "b"}, "b"),
    ],
)
def test_enum_default_is_one_of_the_members(tmp_path: Path, spec: dict, default: str) -> None:
    schema = {
        "definitions": {"g": {"type": "object", "properties": {"p": spec}}},
        "allOf": [{"$ref": "#/definitions/g"}],
    }
    path = tmp_path / "nextflow_schema.json"
    path.write_text(json.dumps(schema))

    cls = get_python_type_inner(
        "args", {"type": "object", "properties": parse_schema(path)}
    )

    assert cls().p.value == default
