from dataclasses import dataclass
from pathlib import Path

import pytest

from latch_cli.nextflow.workflow import flags_from_args, get_flag


@dataclass
class _Arguments:
    validate_params: bool = True


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (False, ["--validate_params", "false"]),
        (True, ["--validate_params"]),
        (None, []),
    ],
)
def test_boolean_flag(value: object, expected: list[str]):
    assert get_flag("validate_params", value) == expected


def test_false_overrides_pipeline_default(tmp_path: Path):
    assert flags_from_args(_Arguments(validate_params=False), tmp_path) == [
        "--validate_params",
        "false",
    ]
