"""Service to list test objects from a managed bucket."""

from typing import List

import boto3

from latch_cli.services.test_data.utils import _retrieve_creds

BUCKET = "latch-public"


def ls() -> List[str]:
    """Lists test data objects.

    Example:

        >>> ls()
            # Will return the full S3 paths of all of my objects.
    """

    session_token, access_key, secret_key, account_id = _retrieve_creds()

    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    objects = s3_resource.Bucket(BUCKET).objects.filter(
        Prefix=f"test-data/{account_id}"
    )
    return [x.key for x in objects]
