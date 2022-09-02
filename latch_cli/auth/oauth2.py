import http.server
import json
import ssl
import urllib
import urllib.request
import webbrowser
from typing import Optional

import certifi

from latch_cli.auth.csrf import CSRFState
from latch_cli.auth.pkce import PKCE
from latch_cli.constants import OAuth2Constants


class OAuth2:
    """An object to facilitate the OAuth2.0 flow.

    This implementation of OAuth2.0 follows `RFC5849`_ as gospel.

    (Note we are using the PKCE extension of OAuth2, but the outline of the
    flow below is still a correct model if crytography is removed.)

    ::

         +--------+                               +---------------+
         |        |--(A)- Authorization Request ->|   Resource    |
         |        |                               |     Owner     |
         |        |<-(B)-- Authorization Grant ---|               |
         |        |                               +---------------+
         |        |
         |        |                               +---------------+
         |        |--(C)-- Authorization Grant -->| Authorization |
         | Client |                               |     Server    |
         |        |<-(D)----- Access Token -------|               |
         |        |                               +---------------+
         |        |
         |        |                               +---------------+
         |        |--(E)----- Access Token ------>|    Resource   |
         |        |                               |     Server    |
         |        |<-(F)--- Protected Resource ---|               |
         +--------+                               +---------------+

                         Figure 1: Abstract Protocol Flow



    Note the correspondence between diagram letters and comments in the code
    example snippet below ::

        # Note these context managers hold values critical to flows.
        with PKCE() as pkce:
            with CSRFState() as csrf_state:

                # Construct our object + call each leg of the flow as a method.
                oauth2_flow = OAuth2(pkce, csrf_state, OAuth2Constants)
                auth_code = oauth2_flow.authorization_request() # A + B
                token = oauth2_flow.access_token_request(auth_code) # C + D

                # With token, we can do E + F...


    Args:
        pkce: Object managing PKCE values.
        csrf_state: Object managing state for CSRF mitigation.
        oauth2_constants: Object holding constants to identify Latch's authz server.

    .. _RFC5849:
        https://datatracker.ietf.org/doc/html/rfc6749
    """

    def __init__(
        self, pkce: PKCE, csrf_state: CSRFState, oauth2_constants: OAuth2Constants
    ):
        self.pkce = pkce
        self.csrf_state = csrf_state
        self.client_id = oauth2_constants.client_id
        self.authz_server_host = oauth2_constants.authz_server_host
        self.redirect_url = oauth2_constants.redirect_url

    def authorization_request(self, connection: Optional[str]) -> str:
        """Request authorization code from Latch authz server.

        Returns:
            An authorization code to complete the first leg of 0Auth2.0.
        """

        class _CallbackHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):

                parsed_url = urllib.parse.urlsplit(self.path)
                if parsed_url.path != "/callback":
                    return

                parsed_query = urllib.parse.parse_qs(parsed_url.query)
                try:
                    code, state = parsed_query["code"][0], parsed_query["state"][0]
                except (KeyError, IndexError):
                    return

                self.send_response(200)
                self.send_header("Content-type", "html")
                self.send_header("Content-Disposition", "inline")
                self.end_headers()

                if state != self.server.csrf_state:
                    # TODO: log csrf attack for debugging but do not fail
                    return

                self.server.authorized = True
                self.server.code = code
                self.server.state = state

                auth_response_html = """
                <!DOCTYPE html>
                <html lang="en">
                  <head>
                    <title>Success</title>
                  </h/ead>
                  <body>
                    <h1>Success</h1>
                    <p>Your Latch SDK has been authenticated.</p>
                  </body>
                </html>
                """.encode(
                    "utf-8"
                )
                self.wfile.write(auth_response_html)

            def log_request(self, format, *args):
                """Quiets server."""
                ...

        url_parameters = {
            "scope": "openid profile email",
            "response_type": "code",
            "redirect_uri": self.redirect_url,
            "client_id": self.client_id,
            "code_challenge": self.pkce.challenge,
            "code_challenge_method": self.pkce.challenge_method,
            "state": self.csrf_state.state,
        }
        if connection is not None:
            url_parameters["connection"] = connection

        base_url = self.authz_server_host + "/authorize?"
        url = base_url + urllib.parse.urlencode(url_parameters)
        webbrowser.open_new(url)

        server_name = ("", 5050)
        server = http.server.HTTPServer(server_name, _CallbackHandler)
        server.authorized = False
        server.csrf_state = self.csrf_state.state
        while not server.authorized:
            server.handle_request()

        return server.code

    def access_token_request(self, auth_code: str) -> str:
        """Using a valid code returned from our authz server, request token.

        Args:
            auth_code: Returned from our authz server if it likes us.
        Returns:
            An access token that a user can use to access their resources on
                latch (register workflows, upload files, etc.)
        """

        token_url = self.authz_server_host + "/oauth/token"
        token_body: bytes = json.dumps(
            {
                "grant_type": "authorization_code",
                "client_id": self.client_id,
                "code_verifier": self.pkce.verifier,
                "code": auth_code,
                "redirect_uri": self.redirect_url,
            }
        ).encode("utf-8")
        token_request = urllib.request.Request(token_url, token_body)
        token_request.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(
            token_request, context=ssl.create_default_context(cafile=certifi.where())
        ) as f:
            try:
                json_response = json.loads(f.read().decode("utf-8"))
                id_token = json_response["id_token"]
            except KeyError:
                raise ValueError(f"Passed response is not json {json_response}")

        return id_token
