import secrets

from latch_cli.auth.utils import _base64url_encode


class CSRFState:

    """Context manager to hold state preventing CSRF attacks.

    Outlined in detail `here`_, this object holds a state string as a
    client-side protection against CSRF attacks.

    Used in conjunction with PKCE verifier/challenge protection, which is a
    server-side security mechanism.

    Example usage: ::

        with CSRFState() as csrf:
            oauth2_flow = OAuth2(..., csrf, ...)

    .. _here:
        https://spring.io/blog/2011/11/30/cross-site-request-forgery-and-oauth2
    """

    def __init__(self):
        self.state = _base64url_encode(secrets.token_bytes(32))

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        ...
