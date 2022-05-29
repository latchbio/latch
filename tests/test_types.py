import pytest

from latch.types.utils import InvalidLatchURL, _validate_latch_url


def test_validate_latch_url():
    valid_urls = (
        "latch:///foo.txt",
        "latch:///foo/bar.txt",
        "latch:///foo/bar/",
        "latch:///foo/bar",
    )
    invalid_urls = ("latch://foo.txt", "lach:///foo.txt")

    for url in valid_urls:
        _validate_latch_url(url)

    for url in invalid_urls:
        with pytest.raises(InvalidLatchURL):
            _validate_latch_url(url)
