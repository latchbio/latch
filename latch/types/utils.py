from pathlib import Path
from typing import Union
from urllib.parse import urlparse


def _is_valid_url(raw_url: Union[str, Path]) -> bool:
    """A valid URL (as a source or destination of a LatchFile) must:
    * contain a latch or s3 scheme
    * contain an absolute path
    """
    try:
        parsed = urlparse(str(raw_url))
    except ValueError:
        return False
    if parsed.scheme not in ("latch", "s3"):
        return False
    if parsed.path != "" and not parsed.path.startswith("/"):
        return False
    return True
