"""Service to remove test objects from a managed bucket."""

import boto3
import botocore
import click

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
    buck = s3_resource.Bucket(BUCKET)

    object_key = object_url[len(f"s3://{BUCKET}/") :]
    if object_key[-1] != "/":
        obj = buck.Object(object_key)

        try:
            obj.load()
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] != "404":
                raise

            click.secho("Object does not exist", fg="red")

            objects = buck.objects.filter(Prefix=object_key + "/").limit(1)
            page = next(objects.pages())
            # page = next(objects.pages(), [])
            if len(page) > 0:
                click.secho(
                    f"Did you mean to remove the directory `{object_url}/`?",
                    fg="yellow",
                )

            raise click.Abort from err

        obj.delete()  # note(maximsmol): succeeds on missing objects
    else:
        objects = buck.objects.filter(Prefix=object_key)

        total_n = 0
        for page in objects.pages():
            total_n += len(page)

        if total_n == 0:
            click.secho("Directory does not exist", fg="red")
            raise click.Abort

        if not click.confirm(
            click.style(f"OK to remove {total_n} child objects?", fg="red")
        ):
            return

        objects.delete()
