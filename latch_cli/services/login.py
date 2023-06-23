"""Service to login."""


from typing import Optional

import click
from latch_sdk_config.latch import config
from latch_sdk_config.user import user_config


def login(connection: Optional[str] = None) -> str:
    """Authenticates a user with Latch and persists an access token.

    Kicks off the three-legged OAuth2.0 flow outlined in `this RFC`_.  The logic
    scaffolding this flow and detailed documentation about it can be found in
    the `latch.auth` package.

    The user will be redirected to a browser and prompted to login. This
    function meanwhile spins up a callback server on a separate thread that will
    be hit when the browser login is successful with an access token.

    .. _this RFC:
        https://datatracker.ietf.org/doc/html/rfc6749
    """
    if _browser_available() is False:
        token: str = click.prompt(
            f"Go to `{config.console_routes.developer}` and copy your API Key here",
            type=str,
        )
        token = token.strip()
        user_config.update_token(token)

        return token

    from latch_cli.auth import PKCE, CSRFState, OAuth2
    from latch_cli.constants import oauth2_constants

    with PKCE() as pkce:
        with CSRFState() as csrf_state:
            oauth2_flow = OAuth2(pkce, csrf_state, oauth2_constants)
            auth_code = oauth2_flow.authorization_request(connection)
            jwt = oauth2_flow.access_token_request(auth_code)

            # Exchange JWT from Auth0 for a persistent token issued by
            # LatchBio.
            access_jwt = _auth0_jwt_for_access_jwt(jwt)
            user_config.update_token(access_jwt)

            return access_jwt


def _browser_available() -> bool:
    """Returns true if browser available for login flow.

    Takes advantage of browser searching logic for many platforms written
    `here`_.

    .. _here:
        https://github.com/python/cpython/blob/3a2b89580ded72262fbea0f7ad24096a90c42b9c/Lib/webbrowser.py#L38
    """
    import webbrowser

    try:
        browser = webbrowser.get()
        if browser is not None:
            return True
    except Exception:
        pass
    return False


def _auth0_jwt_for_access_jwt(token) -> str:
    """Requests a LatchBio minted (long-lived) acccess JWT.

    Uses an Auth0 token to authenticate the user.
    """
    import latch_cli.tinyrequests as tinyrequests

    headers = {
        "Authorization": f"Bearer {token}",
    }

    url = config.api.user.jwt

    response = tinyrequests.post(url, headers=headers)

    resp = response.json()
    try:
        jwt = resp["jwt"]
    except KeyError as e:
        raise ValueError(
            f"Malformed response from request for access token {resp}"
        ) from e

    return jwt
