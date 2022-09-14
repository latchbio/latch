import json
from typing import Tuple

import latch_cli.tinyrequests as tinyrequests
from latch_cli.config.latch import LatchConfig
from latch_cli.utils import current_workspace, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def _retrieve_creds() -> Tuple[str, str, str, str]:

    url = endpoints["get-test-data-creds"]
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}

    response = tinyrequests.post(
        url,
        headers=headers,
        json={
            "ws_account_id": current_workspace(),
        },
    )
    if response.status_code != 200:
        raise ValueError(
            "Unable to retrieve upload credentials. Server responded with {response.json}."
        )

    try:
        output = response.json()
        session_token = output["tmp_session_token"]
        access_key = output["tmp_access_key"]
        secret_key = output["tmp_secret_key"]
    except (json.decoder.JSONDecodeError, AttributeError) as e:
        raise ValueError(
            "Malformed response from server attempting to retrieve upload credentials."
        ) from e

    account_id = current_workspace()

    return session_token, access_key, secret_key, account_id
