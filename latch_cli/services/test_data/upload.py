"""Service to upload test objects to a managed bucket."""

import json
from pathlib import Path

import boto3

import latch_cli.tinyrequests as tinyrequests
from latch_cli.config.latch import LatchConfig
from latch_cli.utils import account_id_from_token, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints

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

    url = endpoints["get-test-data-creds"]
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}

    response = tinyrequests.post(url, headers=headers, json={})
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

    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    account_id = account_id_from_token(token)
    s3_resource.meta.client.upload_file(
        src_path, BUCKET, str(Path(account_id).join(src_path))
    )
