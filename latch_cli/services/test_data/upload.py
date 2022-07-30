"""Service to upload test objects to a managed bucket."""

import sys
from pathlib import Path

import boto3
import botocore

from latch_cli.services.test_data.utils import _retrieve_creds

BUCKET = "latch-public"


def upload(src_path: str) -> str:
    """Uploads a local file to a managed bucket.

    Args:
        src_path: The path of the file to upload.
    Returns: s3 URL of uploaded object.

    Example: ::

        upload("./foo.txt")

            Puts the file `foo.txt` in a managed bucket and returns a publicly
            accesible S3 URL.
    """

    src_path_p = Path(src_path).resolve()

    session_token, access_key, secret_key, account_id = _retrieve_creds()

    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    allowed_key = str((Path("test-data") / account_id).joinpath(src_path))

    try:
        s3_resource.Object(BUCKET, allowed_key).load()
    except botocore.exceptions.ClientError:
        pass
    else:
        confirm = input("Object already exists, override it?  (y/n) > ")
        if confirm in ("n", "no"):
            print("Aborting upload.")
            sys.exit()
        elif confirm in ("y", "yes"):
            pass
        else:
            print("Invalid response.")
            sys.exit()

    s3_resource.meta.client.upload_file(src_path, BUCKET, allowed_key)

    return f"s3://{BUCKET}/{allowed_key}"
