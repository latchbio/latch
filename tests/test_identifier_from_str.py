import latch  # ruff: ignore[unused-import]
from latch_cli.utils import identifier_from_str


def test_identifier_from_empty_string() -> None:
    assert identifier_from_str("").isidentifier()


def test_identifier_from_str_unchanged_for_normal_input() -> None:
    assert identifier_from_str("peptide") == "peptide"
    assert identifier_from_str("a-b") == "a_b"
