from enum import Enum
from urllib.parse import urlparse


class LatchSchemes(Enum):
    latch = "latch"
    s3 = "s3"


class URL:
    """Validates a URL string with respect to a scheme.

    Args:
        scheme : eg. s3, latch
        raw_url : the url string to be validated
    """

    def __init__(self, scheme: str, raw_url: str):
        scheme = scheme.value
        raw_scheme = urlparse(raw_url).scheme
        if raw_scheme != scheme:
            raise ValueError(f"{raw_url} is must use the {scheme} scheme.")
        self._url = raw_url

    @property
    def url(self) -> str:
        """Returns self as string."""
        return self._url


class LatchURL(URL):
    """A URL referencing an object in LatchData.

    Uses the latch scheme and a path that resolves absolutely with
    respect to an authenticated users's root.

    ..
        latch:///foobar # a valid directory
        latch:///test_samples/test.fa # a valid file
    """

    def __init__(self, raw_url: str):
        super().__init__(LatchSchemes.latch, raw_url)


class S3URL(URL):
    """A URL referencing an object in S3.

    ..
        s3:/<bucket>//path
    """

    def __init__(self, raw_url: str):
        super().__init__(LatchSchemes.s3, raw_url)
