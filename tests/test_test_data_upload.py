from pathlib import Path

import botocore.exceptions
import pytest

from latch_cli.services.test_data import upload as upload_mod


class _FakeS3:
    def __init__(self) -> None:
        self.keys: list[str] = []

    @staticmethod
    def head_object(**_: object) -> None:
        raise botocore.exceptions.ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def upload_file(self, _src: str, _bucket: str, key: str) -> None:
        self.keys.append(key)


@pytest.fixture
def fake_s3(monkeypatch: pytest.MonkeyPatch) -> _FakeS3:
    s3 = _FakeS3()
    monkeypatch.setattr(
        upload_mod, "_retrieve_creds", lambda: ("token", "access", "secret", "123")
    )
    monkeypatch.setattr(upload_mod.boto3, "client", lambda *_, **__: s3)
    return s3


def test_upload_absolute_path_stays_under_account_prefix(
    fake_s3: _FakeS3, tmp_path: Path
):
    src = tmp_path / "adata.h5ad"
    src.write_text("x")

    url = upload_mod.upload(str(src))

    assert len(fake_s3.keys) == 1
    key = fake_s3.keys[0]
    assert key.startswith("test-data/123/")
    assert key.endswith("/adata.h5ad")
    assert "\\" not in key
    assert url == f"s3://latch-public/{key}"


def test_upload_relative_dir_uses_posix_keys(
    fake_s3: _FakeS3, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    (tmp_path / "data" / "sub").mkdir(parents=True)
    (tmp_path / "data" / "sub" / "reads.fastq").write_text("x")
    monkeypatch.chdir(tmp_path)

    url = upload_mod.upload(str(Path("data") / "sub"))

    assert fake_s3.keys == ["test-data/123/data/sub/reads.fastq"]
    assert url == "s3://latch-public/test-data/123/data/sub"
