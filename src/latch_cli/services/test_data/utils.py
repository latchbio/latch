import json
from typing import Tuple

from latch_sdk_config.latch import config

import latch_cli.tinyrequests as tinyrequests
from latch.utils import current_workspace, retrieve_or_login


def _retrieve_creds() -> Tuple[str, str, str, str]:
    response = tinyrequests.post(
        config.api.data.test_data,
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "ws_account_id": current_workspace(),
        },
    )
    if response.status_code != 200:
        raise ValueError(
            "Unable to retrieve upload credentials. Server responded with"
            f" {response.json}."
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
