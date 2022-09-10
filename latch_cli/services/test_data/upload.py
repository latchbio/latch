"""Service to upload test objects to a managed bucket."""

from pathlib import Path

import boto3
import botocore
import click

from latch_cli.services.test_data.utils import _retrieve_creds

BUCKET = "latch-public"


def upload(src_path: str, dont_confirm_overwrite: bool = True) -> str:
    """Uploads a local file/folder to a managed bucket.

    Args:
        src_path: The path of the file/folder to upload.
    Returns: s3 URL of uploaded object.

    Example: ::

        upload("./foo.txt")

            Puts the file `foo.txt` in a managed bucket and returns a publicly
            accesible S3 URL.
    """

    session_token, access_key, secret_key, account_id = _retrieve_creds()

    s3_resource = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    allowed_key = str((Path("test-data") / account_id).joinpath(src_path))

    upload_helper(
        Path(src_path).resolve(),
        allowed_key,
        s3_resource,
        dont_confirm_overwrite,
    )

    return f"s3://{BUCKET}/{allowed_key}"


def upload_helper(
    src_path: Path,
    key: str,
    s3_resource,
    dont_confirm_overwrite: bool,
):
    if not src_path.exists():
        raise ValueError(f"Path {src_path} doesn't exist.")

    if src_path.is_dir():
        for sub_path in src_path.iterdir():
            upload_helper(
                sub_path, f"{key}/{sub_path.name}", s3_resource, dont_confirm_overwrite
            )
    else:
        upload_file(src_path, key, s3_resource, dont_confirm_overwrite)


def upload_file(
    src_path: Path,
    key: str,
    s3_resource,
    dont_confirm_overwrite: bool,
):
    try:
        s3_resource.head_object(Bucket=BUCKET, Key=key)
    except botocore.exceptions.ClientError:
        pass
    else:
        while True and not dont_confirm_overwrite:
            confirm = input(f"{key} already exists, override it?  (y/n) > ")
            if confirm in ("n", "no"):
                print("Aborting upload.")
                return
            elif confirm in ("y", "yes"):
                break
            else:
                print("Invalid response.")

    s3_resource.upload_file(str(src_path), BUCKET, key)
    click.secho(f"Successfully uploaded {key}", fg="green")
