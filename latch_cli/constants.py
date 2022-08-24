"""Package-wide constants."""

from dataclasses import dataclass


@dataclass
class OAuth2Constants:

    client_id: str = "jzFBOhIbfp4EPRYZ8wmx4YyvL27LFDeB"
    """Identifies the authentication server in 0Auth2.0 flow"""

    authz_server_host: str = "https://latchai.us.auth0.com"
    """Host of the authentication server used in 0Auth2.0 flow."""

    redirect_url: str = "http://127.0.0.1:5050/callback"
    """Redirect URL registered with authentication server."""


MAX_FILE_SIZE = 4 * 2**20
