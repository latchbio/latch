"""Service to login."""

from datetime import datetime
from typing import Optional

import click
import gql

from latch.utils import account_id_from_token
from latch_sdk_config.latch import config
from latch_sdk_config.user import user_config
from latch_sdk_gql.execute import execute


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

    if user_config.token != "":
        try:
            account_id = account_id_from_token(user_config.token)
            res = execute(
                gql.gql("""
                    query AccountIdToDisplayName($accountId: BigInt!) {
                        userInfoByAccountId(accountId: $accountId) {
                            id
                            name
                        }
                        teamInfoByAccountId(accountId: $accountId) {
                            accountId
                            displayName
                        }
                    }
                """),
                {"accountId": account_id},
            )
            entity_string = None
            if res["userInfoByAccountId"] is not None:
                user_name = res["userInfoByAccountId"]["name"]
                entity_string = f"user {user_name}"
            elif res["teamInfoByAccountId"] is not None:
                team_info = res["teamInfoByAccountId"]["displayName"]
                entity_string = f"team {team_info}"
            else:
                raise ValueError("absurd")

            if click.confirm(f"Found token for {entity_string}, use it?", default=True):
                return user_config.token

            click.secho(
                "Generating new token. If old token is unused, please revoke it from the Latch console (https://console.latch.bio/settings/developer).",
                fg="yellow",
            )

        except Exception:  # noqa: BLE001, S110
            pass

    if _browser_available() is False:
        token: str = click.prompt(
            f"Go to `{config.console_routes.developer}`, generate a Personal API Token (or Workspace API Token if you only need to access a single workspace from this machine), and paste it here",
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

            latch_sdk_token = _auth0_jwt_for_latch_sdk_token(jwt)
            user_config.update_token(latch_sdk_token)

            return latch_sdk_token


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


def _auth0_jwt_for_latch_sdk_token(auth0_token: str) -> str:
    res = execute(
        gql.gql("""
            query AccountIdFromToken {
                accountInfoCurrent {
                    id
                }
            }
        """),
        auth_header=f"Bearer {auth0_token}",
    )
    aic = res.get("accountInfoCurrent")
    if aic is None or aic.get("id") is None:
        raise ValueError(
            "Your Latch access token is invalid or could not be resolved to an account."
        )
    account_id = aic["id"]

    mint_res = execute(
        gql.gql("""
            mutation ApiKeyCreate(
                $accountId: BigInt!
                $displayName: String!
            ) {
            apiKeyCreate(
                input: {
                    argAccountId: $accountId
                    argDisplayName: $displayName
                }
            ) {
                result {
                        token
                    }
                }
            }
        """),
        {
            "accountId": account_id,
            "displayName": f"CLI Login on {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z%z')}",
        },
        auth_header=f"Bearer {auth0_token}",
    )
    minted_token = mint_res.get("apiKeyCreate")
    if minted_token is None or minted_token.get("result") is None:
        raise ValueError("Failed to generate a token for the user account id.")

    return minted_token.get("result").get("token")
