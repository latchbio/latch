"""Service to remove test objects from a managed bucket."""

import boto3
import botocore

from latch_cli.services.test_data.utils import _retrieve_creds

BUCKET = "latch-public"


def remove(object_url: str):
    """Removes an object from a managed bucket.

    Args:
        object_url: The url of the object to remove.

    Example:

        >>> remove("s3://latch-public/1/foo.txt")
            # Removes the object at this path from your managed bucket prefix.
    """

    session_token, access_key, secret_key, account_id = _retrieve_creds()

    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )
    object_key = object_url[len(f"s3://{BUCKET}/") :]

    try:
        s3_resource.Object(BUCKET, object_key).load()
    except botocore.exceptions.ClientError:
        raise ValueError(f"{object_url} does not exist")

    s3_resource.Object(BUCKET, object_key).delete()
