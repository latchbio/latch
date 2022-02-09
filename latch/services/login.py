"""Service to login."""

from latch.auth import PKCE, CSRFState, OAuth2
from latch.config import UserConfig
from latch.constants import OAuth2Constants


def login() -> str:
    """Authenticates user with Latch and persists an access token.

    Kicks off a three-legged OAuth2.0 flow outlined in `this RFC`_.  Logic
    scaffolding this flow and detailed documentation can be found in the
    `latch.auth` package

    From a high-level, the user will be redirected to a browser and prompted to
    login. The SDK meanwhile spins up a callback server on a separate thread
    that will be hit when the browser login is successful with an access token.

    .. _this RFC:
        https://datatracker.ietf.org/doc/html/rfc6749
    """

    with PKCE() as pkce:
        with CSRFState() as csrf_state:

            oauth2_flow = OAuth2(pkce, csrf_state, OAuth2Constants)
            auth_code = oauth2_flow.authorization_request()
            token = oauth2_flow.access_token_request(auth_code)

            config = UserConfig()
            config.update_token(token)
            return token
