import json
import os

import latch.gql as gql


class Account:
    def __init__(
        self,
        account_id: str,
        workspace_id: str,
    ):
        self.account_id = account_id
        self.workspace_id = workspace_id

    @classmethod
    def current(cls):
        # execution_token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
        execution_token = "fd9c4aed6a5a1422f804"

        info = gql.execute(
            document="""
                query accountInfoQuery ($argExecutionToken: String!) {
                    executionCreatorByToken(token: $argExecutionToken) {
                        createdBy
                        launchedBy
                    }
                }
            """,
            variables={"argExecutionToken": execution_token},
        )["executionCreatorByToken"]

        return cls(
            account_id=info["launchedBy"],
            workspace_id=info["createdBy"],
        )
