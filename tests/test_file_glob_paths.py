from pathlib import Path

import pytest

from latch.types.glob import file_glob


@pytest.mark.parametrize(
    "remote_directory",
    [
        "latch:///fastqc_outputs",
        "latch:///fastqc_outputs/",
        "latch://1.account/regions",
        "s3://bucket/regions",
    ],
)
@pytest.mark.parametrize("name", ["a.fastq.gz", "chr1:100-200.bed"])
def test_file_glob_separates_remote_directory_and_name(
    tmp_path: Path, remote_directory: str, name: str
):
    local_file = tmp_path / name
    local_file.touch()
    files = file_glob("*", remote_directory, target_dir=tmp_path)
    assert len(files) == 1
    expected = (
        remote_directory if remote_directory.endswith("/") else f"{remote_directory}/"
    )
    assert files[0].remote_path == f"{expected}{name}"
    assert files[0].path == str(local_file)


def test_file_glob_preserves_root_url(tmp_path: Path):
    (tmp_path / "a.txt").touch()
    files = file_glob("*", "latch:///", target_dir=tmp_path)
    assert files[0].remote_path == "latch:///a.txt"


def test_file_glob_invalid_remote_returns_empty(tmp_path: Path):
    (tmp_path / "a.txt").touch()
    assert file_glob("*", "relative/path", target_dir=tmp_path) == []
