"""
csrf
~~~~
Context manager to hold state preventing CSRF attacks.
"""

import secrets

from latch.auth.utils import base64url_encode


class CSRFState:
    """A state string as a client-side protection against CSRF attacks.

    Used in conjunction with PKCE verifier/challenge protection, which is a
    server-side security mechanism.

    https://spring.io/blog/2011/11/30/cross-site-request-forgery-and-oauth2
    """

    def __init__(self):
        self.state = base64url_encode(secrets.token_bytes(32))

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        ...
