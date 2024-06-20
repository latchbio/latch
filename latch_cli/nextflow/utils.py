import os
from typing import Optional

import gql
from latch_sdk_gql.execute import execute


def _get_execution_name() -> Optional[str]:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        return None

    res = execute(
        gql.gql("""
        query executionCreatorsByToken($token: String!) {
            executionCreatorByToken(token: $token) {
                flytedbId
                info {
                    displayName
                }
            }
        }
        """),
        {"token": token},
    )["executionCreatorByToken"]

    if "info" not in res or res["info"] is None:
        return None
    return res["info"].get("displayName")
