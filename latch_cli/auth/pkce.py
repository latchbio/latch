import hashlib
import secrets
from typing import Tuple

from latch_cli.auth.utils import base64url_encode


class PKCE:
    """A Context manager to control state for PKCE flow.

    The Proof Key for Code Exchange is outlined rigorously in `RFC7636`_.  The
    implementation in this module was written using that document as gospel. It
    is an extension of OAuth2.0 that adds crytographic secrets to prevent
    request interception.

    Here is a diagram to summarize the flow (capital letters correspond to
    descriptions):

    ::

                                                      +-------------------+
                                                      |   Authz Server    |
            +--------+                                | +---------------+ |
            |        |--(A)- Authorization Request ---->|               | |
            |        |       + t(code_verifier), t_m  | | Authorization | |
            |        |                                | |    Endpoint   | |
            |        |<-(B)---- Authorization Code -----|               | |
            |        |                                | +---------------+ |
            | Client |                                |                   |
            |        |                                | +---------------+ |
            |        |--(C)-- Access Token Request ---->|               | |
            |        |          + code_verifier       | |    Token      | |
            |        |                                | |   Endpoint    | |
            |        |<-(D)------ Access Token ---------|               | |
            +--------+                                | +---------------+ |
                                                      +-------------------+

                          Figure 2: Abstract Protocol Flow


    A. The client creates and records a secret named the "code_verifier"
       and derives a transformed version "t(code_verifier)" (referred to
       as the "code_challenge"), which is sent in the OAuth 2.0
       Authorization Request along with the transformation method "t_m".

    B. The Authorization Endpoint responds as usual but records
       "t(code_verifier)" and the transformation method.

    C. The client then sends the authorization code in the Access Token
       Request as usual but includes the "code_verifier" secret generated
       at (A).

    D. The authorization server transforms "code_verifier" and compares
       it to "t(code_verifier)" from (B).  Access is denied if they are
       not equal.

    An attacker who intercepts the authorization code at (B) is unable to
    redeem it for an access token, as they are not in possession of the
    "code_verifier" secret.

    Example usage: ::

        with PKCE() as pkce:
            oauth2_flow = OAuth2(pkce, ..)


    .. _RFC7636:
        https://datatracker.ietf.org/doc/html/rfc7636
    """

    challenge_method = "S256"
    """The challenge method used to encode the code verifier.

    'If the client is capable of using "S256", it MUST use "S256", as
    "S256" is Mandatory To Implement (MTI) on the server.'
        
    .. _RFC7636#section-4.2:
        https://datatracker.ietf.org/doc/html/rfc7636#section-4.2

    Thus this value is hardcoded.
    """

    def __init__(self):
        self.verifier, self.challenge = self.construct_challenge()

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        ...

    def construct_challenge(self) -> Tuple[str, str]:
        """Construct verifier & challenge to verify a client's identity in PKCE.

        Reference  `RFC7636`_.

        Returns:
            code verifier: A cryptographically random string that is used to
                correlate the authorization request to the token request.
            code challenge: A challenge derived from the code verifier that is
                sent in the authorization request, to be verified against later.

        .. _RFC7636:
            https://datatracker.ietf.org/doc/html/rfc7636
        """
        verifier = base64url_encode(secrets.token_bytes(32))
        challenge = base64url_encode(hashlib.sha256(verifier.encode()).digest())
        return verifier, challenge
