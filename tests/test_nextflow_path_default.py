import json
from pathlib import Path

from latch_cli.nextflow.config import get_python_type_inner
from latch_cli.nextflow.parse_schema import parse_schema


def test_pipeline_relative_path_default_does_not_crash(tmp_path: Path) -> None:
    schema = {
        "definitions": {
            "g": {
                "type": "object",
                "properties": {
                    "filter_config": {
                        "type": "string",
                        "format": "path",
                        "default": "$projectDir/assets/filter.json",
                    }
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

    # the default cannot be expressed as a Latch path, so the parameter is treated like
    # any optional parameter without a default (None -> no flag -> nextflow's own default)
    assert cls(filter_config=None).filter_config is None
