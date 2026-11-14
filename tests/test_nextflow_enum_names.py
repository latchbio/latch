import json
from pathlib import Path

from latch_cli.nextflow.config import get_python_type_inner
from latch_cli.nextflow.parse_schema import parse_schema


def test_enum_values_with_same_identifier_stay_distinct(tmp_path: Path) -> None:
    schema = {
        "definitions": {
            "g": {
                "type": "object",
                "properties": {
                    "gram": {"type": "string", "default": "?", "enum": ["+", "-", "?"]}
                },
            }
        },
        "allOf": [{"$ref": "#/definitions/g"}],
    }
    path = tmp_path / "nextflow_schema.json"
    path.write_text(json.dumps(schema))

    cls = get_python_type_inner(
        "args", {"type": "object", "properties": parse_schema(path)}
    )
    enum_cls = cls.__dataclass_fields__["gram"].type.__args__[0].__args__[0]

    assert [m.value for m in enum_cls] == ["+", "-", "?"]
    assert cls().gram.value == "?"
