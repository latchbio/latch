import json
from pathlib import Path

import click
import pytest

from latch_cli.nextflow.parse_schema import parse_schema

SCHEMA = {
    "definitions": {
        "g": {
            "type": "object",
            "properties": {
                # `help` as declared by the nf-core pipeline template
                "help": {"type": ["boolean", "string"]},
                "mode": {"type": ["string", "null"], "enum": ["a", "b"]},
                "outdir": {"type": "string", "format": "directory-path"},
            },
        }
    },
    "allOf": [{"$ref": "#/definitions/g"}],
}


def test_unsupported_type_is_skipped_when_not_strict(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "nextflow_schema.json"
    path.write_text(json.dumps(SCHEMA))

    parsed = parse_schema(path, strict=False)

    assert list(parsed) == ["outdir"]
    assert "unable to parse 'help'" in capsys.readouterr().out


def test_unsupported_type_is_reported_when_strict(tmp_path: Path) -> None:
    path = tmp_path / "nextflow_schema.json"
    path.write_text(json.dumps(SCHEMA))

    with pytest.raises(click.exceptions.Exit):
        parse_schema(path, strict=True)
