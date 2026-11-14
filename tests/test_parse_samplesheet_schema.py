import json
from pathlib import Path

import pytest

from latch_cli.nextflow.parse_schema import parse_schema


def test_file_param_with_non_object_items_schema_is_a_plain_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "assets").mkdir()
    # nf-core/fetchngs: the input file is a list of identifiers, one per line
    (tmp_path / "assets" / "schema_input.json").write_text(
        json.dumps({
            "type": "array",
            "items": {"type": "string", "pattern": "^SRR\\d+$"},
        })
    )
    schema = {
        "definitions": {
            "g": {
                "type": "object",
                "properties": {
                    "input": {
                        "type": "string",
                        "format": "file-path",
                        "schema": "assets/schema_input.json",
                    }
                },
            }
        },
        "allOf": [{"$ref": "#/definitions/g"}],
    }
    (tmp_path / "nextflow_schema.json").write_text(json.dumps(schema))

    parsed = parse_schema(tmp_path / "nextflow_schema.json")

    assert parsed["input"]["type"] == "blob"
    assert parsed["input"]["node_type"] == "file"
