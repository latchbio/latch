import pytest

import latch  # ruff: ignore[unused-import]
import latch_cli.services.cp.glob as glob_mod


def test_expand_pattern_matches_whole_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        glob_mod,
        "_get_immediate_children_of_node",
        lambda _parent: ["a.fq", "a.fq.gz", "a.fq.bak", "notes.fq.txt", "b.fastq"],
    )

    assert glob_mod.expand_pattern("latch://1.account/data/*.fq") == [
        "latch://1.account/data/a.fq"
    ]


def test_expand_pattern_without_wildcard_is_unchanged() -> None:
    assert glob_mod.expand_pattern("latch://1.account/data/a.fq") == [
        "latch://1.account/data/a.fq"
    ]
