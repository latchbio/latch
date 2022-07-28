"""Service to upload test objects to a managed bucket."""

from pathlib import Path

import boto3

from latch_cli.services.test_data.utils import _retrieve_creds

BUCKET = "latch-public"


def upload(src_path: str):
    """Uploads a local file to a managed bucket.

    Args:
        src_path: The path of the file to upload.

    Example: ::

        upload("./foo.txt")

            Puts the file `foo.txt` in a managed bucket and returns a publicly
            accesible S3 URL.
    """

    src_path_p = Path(src_path).resolve()
    if src_path_p.exists() is not True:
        raise ValueError(f"{src_path} must exist.")

    session_token, access_key, secret_key, account_id = _retrieve_creds()

    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    allowed_key = str(Path(account_id).joinpath(src_path))
    s3_resource.meta.client.upload_file(src_path, BUCKET, allowed_key)
