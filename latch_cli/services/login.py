"""Service to login."""


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

    from latch_cli.auth import PKCE, CSRFState, OAuth2
    from latch_cli.constants import OAuth2Constants

    with PKCE() as pkce:
        with CSRFState() as csrf_state:

            oauth2_flow = OAuth2(pkce, csrf_state, OAuth2Constants)
            auth_code = oauth2_flow.authorization_request()
            jwt = oauth2_flow.access_token_request(auth_code)

            # Exchange JWT from Auth0 for a persistent token issued by
            # LatchBio.
            access_jwt = _auth0_jwt_for_access_jwt(jwt)

            from latch_cli.config.user import UserConfig

            config = UserConfig()

            config.update_token(access_jwt)
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
    from latch_cli.config.latch import LatchConfig

    endpoints = LatchConfig().sdk_endpoints

    headers = {
        "Authorization": f"Bearer {token}",
    }

    url = endpoints["access-jwt"]

    response = tinyrequests.post(url, headers=headers)

    resp = response.json()
    try:
        jwt = resp["jwt"]
    except KeyError as e:
        raise ValueError(
            f"Malformed response from request for access token {resp}"
        ) from e

    return jwt
