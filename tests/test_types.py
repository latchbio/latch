import pytest

from latch.types.utils import _is_valid_url


def test_validate_latch_url():
    valid_urls = (
        "latch:///foo.txt",
        "latch:///foo/bar.txt",
        "latch:///foo/bar/",
        "latch:///foo/bar",
        "s3:///foo/bar",
    )
    invalid_urls = ("latch://foo.txt", "lach:///foo.txt", "gcp:///foo.txt")

    for url in valid_urls:
        assert _is_valid_url(url) is True

    for url in invalid_urls:
        assert _is_valid_url(url) is False
