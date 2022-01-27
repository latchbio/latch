"""
oauth2
~~~~~~
An object to facilitate the two-legged OAuth2.0 flow.
"""

import http.server
import json
import urllib
import urllib.request
import webbrowser

from latch.auth.csrf import CSRFState
from latch.auth.pkce import PKCE
from latch.constants import OAuth2Constants


class OAuth2:
    """TODO: docs on flow..."""

    def __init__(
        self, pkce: PKCE, csrf_state: CSRFState, oauth2_constants: OAuth2Constants
    ):
        self.pkce = pkce
        self.csrf_state = csrf_state
        self.client_id = oauth2_constants.client_id
        self.authz_server_host = oauth2_constants.authz_server_host
        self.redirect_url = oauth2_constants.redirect_url

    def authorization_request(self) -> str:
        """TODO: Returns code..."""

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
        with urllib.request.urlopen(token_request) as f:
            try:
                json_response = json.loads(f.read().decode("utf-8"))
                id_token = json_response["id_token"]
            except KeyError:
                raise ValueError(f"Passed response is not json {json_response}")

        return id_token
