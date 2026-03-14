import re
from dataclasses import asdict
from pathlib import Path
from textwrap import dedent
from typing import Optional

import click
import docker.auth
import gql

from latch.utils import current_workspace
from latch_cli.services.register.register import print_upload_logs
from latch_sdk_gql.execute import execute

from ..utils import hash_directory
from .docker.utils import dbnp, get_credentials, get_local_docker_client, remote_dbnp

ecr_base = "812206152185.dkr.ecr.us-west-2.amazonaws.com"


def record_in_db(ws_id: str, image_name: str, version: str):
    execute(
        gql.gql("""
            mutation AddStagingImage(
                $wsId: BigInt!
                $imageName: String!
                $version: String!
            ) {
                createPrivateImage(
                    input: {
                        privateImage: {
                            workspaceId: $wsId
                            imageName: $imageName
                            version: $version
                        }
                    }
                ) {
                    clientMutationId
                }
            }
        """),
        {"wsId": ws_id, "imageName": image_name, "version": version},
    )


# note(ayush): latch register, etc. do a simplified version of this that can unnecessarily reformat
# image names
valid_image_expr = re.compile(
    r"""
    ^
        [a-z0-9]+
        (
            ([_.]|__|[-]*)
            [a-z0-9]+
        )*
    $
    """,
    re.VERBOSE,
)
valid_version_expr = re.compile(r"[\w][\w.-]{0,127}")
image_ref_expr = re.compile(
    r"((?P<registry>[^/:]+)/)?(?P<image>[^:]+)(:(?P<version>[^:/]))?"
)


def validate_image_name(name: str):
    if valid_image_expr.match(name) is None:
        click.secho(
            dedent(f"""\
                Image name `{name}` is not a valid docker image name. Please ensure that

                1. The image name consists entirely of lowercase letters, numbers, "_", and "-" characters.
                2. The image name does not contain 3 or more consecutive `_` characters

                See https://pkg.go.dev/github.com/distribution/reference#pkg-overview for more info.
            """),
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(1)


def validate_version(version: str):
    if valid_version_expr.match(version) is None:
        click.secho(
            dedent(f"""\
                Version `{version}` is not a valid docker image version. Please ensure that

                1. The version consists entirely of alphanumeric, "_", and "." characters.
                2. The version does not start with a "." character.
                3. The version is at most 128 characters long.

                See https://pkg.go.dev/github.com/distribution/reference#pkg-overview for more info.
            """),
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(1)


# note(ayush): not going to support remote here as remote instance will not have necessary
# credentials if the source image is private and i dont want to deal with federation or forwarding
# credentials
def upload_image(
    image_ref: str,
    *,
    image_name: Optional[str] = None,
    version: Optional[str] = None,
    skip_confirmation: bool = False,
):
    click.secho("Beginning image upload:")
    match = image_ref_expr.match(image_ref)

    if image_name is not None:
        validate_image_name(image_name)
    elif match is not None:
        image_name = match["image"]
    else:
        click.secho(
            dedent(f"""\
            Could not parse image name from reference `{image_ref}`

            Please either provide a human readable image reference (e.g. `registry.dockerhub.io/test_image:123`), or
            pass in a custom image name using `--image-name`.
            """),
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(1)

    if version is not None:
        validate_version(version)
    elif match is not None and match["version"] is not None:
        version = match["version"]
    elif match is not None:
        version = "latest"
    else:
        click.secho(
            dedent(f"""\
            Could not parse image version ame from reference `{image_ref}`

            Please either provide a human readable image reference (e.g. `registry.dockerhub.io/test_image:123`), or
            pass in a custom version using `--version`.
            """),
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(1)

    assert image_name is not None
    assert version is not None

    ws_id = current_workspace()

    namespaced_image_name = f"{ws_id}_{image_name}"

    full_image_ref = f"{ecr_base}/{namespaced_image_name}:{version}"

    click.secho(f"Image Destination: {full_image_ref}")

    if not skip_confirmation and not click.confirm("Proceed?"):
        raise click.Abort

    credentials = get_credentials(namespaced_image_name)
    client = get_local_docker_client()

    print_upload_logs(
        client.pull(image_ref, stream=True, decode=True, platform="linux/amd64"),
        image_ref,
        print_header=False,
    )

    client.tag(image_ref, repository=f"{ecr_base}/{namespaced_image_name}", tag=version)

    client._auth_configs = docker.auth.AuthConfig({  # noqa: SLF001
        "auths": {ecr_base: asdict(credentials)}
    })

    print_upload_logs(
        client.push(
            repository=f"{ecr_base}/{namespaced_image_name}",
            tag=version,
            stream=True,
            decode=True,
            auth_config=asdict(credentials),
        ),
        namespaced_image_name,
    )

    record_in_db(ws_id, namespaced_image_name, version)

    click.secho(f"Successfully built and tagged {full_image_ref}", fg="green")


def build_and_upload_image(
    root: Path,
    *,
    image_name: str,
    version: Optional[str] = None,
    dockerfile_path: Optional[Path] = None,
    remote: bool = True,
    skip_confirmation: bool = False,
    progress_plain: bool = False,
):
    click.secho("Beginning image build and upload:")

    validate_image_name(image_name)

    if version is not None:
        validate_version(version)
    else:
        click.secho(
            "  `--version` not provided: generating hash version instead",
            dim=True,
            italic=True,
        )

        version = hash_directory(root, silent=True)

    ws_id = current_workspace()
    namespaced_image_name = f"{ws_id}_{image_name}"

    full_image_ref = f"{ecr_base}/{namespaced_image_name}:{version}"

    click.secho(f"Image Destination: {full_image_ref}")

    if not skip_confirmation and not click.confirm("Proceed?"):
        raise click.Abort

    if dockerfile_path is None:
        dockerfile_path = root / "Dockerfile"

    if not dockerfile_path.exists():
        click.secho(
            f"No Dockerfile found at `{dockerfile_path}`. Use `--dockerfile` to pass in a custom path.",
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(1)

    click.secho(f"Using dockerfile at `{dockerfile_path}`", dim=True, italic=True)

    if remote:
        remote_dbnp(
            root,
            namespaced_image_name,
            version,
            dockerfile_path,
            progress_plain=progress_plain,
        )
    else:
        client = get_local_docker_client()

        dbnp(
            client,
            root,
            namespaced_image_name,
            version,
            dockerfile_path,
            progress_plain=progress_plain,
        )

    record_in_db(ws_id, namespaced_image_name, version)

    click.secho(f"Successfully built and tagged {full_image_ref}", fg="green")
