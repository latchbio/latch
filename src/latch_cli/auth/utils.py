"""Authorization utilities."""

import base64


def _base64url_encode(bytestring: bytes) -> str:
    """Construct a "base64url" encoding of a bytestring.

    Not that this is a modified Base64 encoding with a url + filename safe
    alphabet. This "base64url" encoding is *not* the same thing as a
    "base64" encoding.

    The differences are enumerated in the `OAuth2 RFC`_.

    Args:
        bytestring: string of bytes to be encoded

    Returns:
        Encoded bytestring

    .. _OAuth2 RFC:
        https://datatracker.ietf.org/doc/html/rfc4648#section-5
    """
    # Padding must be stripped as specified in RFC.
    return base64.urlsafe_b64encode(bytestring).decode("utf-8").replace("=", "")
