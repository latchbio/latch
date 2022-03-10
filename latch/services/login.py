"""Service to login."""

import webbrowser

import requests
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
    if _browser_available() is False:
        raise OSError(
            "Unable to locate a browser on this machine. Unable to facilitate"
            " login flow"
        )

    with PKCE() as pkce:
        with CSRFState() as csrf_state:

            oauth2_flow = OAuth2(pkce, csrf_state, OAuth2Constants)
            auth_code = oauth2_flow.authorization_request()
            jwt = oauth2_flow.access_token_request(auth_code)

            # Exchange JWT from Auth0 for a persistent token issued by
            # LatchBio.
            access_jwt = _auth0_jwt_for_access_jwt(jwt)

            config = UserConfig()
            config.update_token(access_jwt)
            return access_jwt


def _browser_available() -> bool:
    """Returns true if browser available for login flow.

    Takes advantage of browser seaching logic for many platforms written
    `here`_.

    .. _here:
        https://github.com/python/cpython/blob/main/Lib/webbrowser.py#L38
    """
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

    headers = {
        "Authorization": f"Bearer {token}",
    }

    url = "https://nucleus.latch.bio/sdk/access-jwt"

    response = requests.post(url, headers=headers)
    if response.status_code == 403:
        raise PermissionError(
            "You need access to the Latch SDK beta ~ join the waitlist @ https://latch.bio/sdk"
        )

    try:
        resp = response.json()
        jwt = resp["jwt"]
    except KeyError as e:
        raise ValueError(
            f"Malformed response from request for access token {resp}"
        ) from e

    return jwt
