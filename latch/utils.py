"""
utils
~~~~~
Hammers and nails.
"""

import base64

import jwt


def base64url_encode(bytestring: bytes) -> str:
    """Construct a "base64url" encoding of a bytestring.

    Not that this is a modified Base 64 encoding with a url + filename safe
    alphabet. This "base64url" encoding is _not_ the same thing as a
    "base64" encoding.

    The differences are enumerated
    [here](https://datatracker.ietf.org/doc/html/rfc4648#section-5).
    """
    # Padding must be stripped as specified in RFC.
    return base64.urlsafe_b64encode(bytestring).decode("utf-8").replace("=", "")


def sub_from_jwt(token: str) -> str:
    """Extract a user sub from a generic jwt."""

    payload = jwt.decode(token, options={"verify_signature": False})
    try:
        sub = payload["sub"]
    except KeyError:
        raise ValueError(
            "Provided token lacks a user sub in the data payload"
            " and is not a valid token."
        )
    return sub
