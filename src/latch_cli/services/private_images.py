import re
from dataclasses import asdict
from pathlib import Path
from textwrap import dedent, indent
from typing import Optional, TypedDict

import click
import dateutil.parser as dp
import docker.auth
import docker.errors
import gql

from latch.utils import current_workspace, get_workspaces
from latch_sdk_config.latch import config
from latch_sdk_gql.execute import execute

from ..utils import hash_directory, human_readable_datetime
from .docker.utils import dbnp, get_credentials, get_local_docker_client, remote_dbnp
from .register.register import print_upload_logs

ecr_base = config.dkr_repo

# the image reached the registry but the record did not. Distinct from 1 so a caller
# can tell "not published" from "published but unrecorded".
record_failed_exit_code = 3


class PrivateImageNode(TypedDict):
    imageName: str
    version: str
    creationTime: str


class PrivateImages(TypedDict):
    nodes: Optional[list[PrivateImageNode]]


class PrivateImageExistsNode(TypedDict):
    workspaceId: str
    imageName: str
    version: str


class PrivateImageExistsResult(TypedDict):
    nodes: Optional[list[PrivateImageExistsNode]]


def is_recorded_in_db(ws_id: str, image_name: str, version: str) -> bool:
    """Report whether the workspace already has a record of this image and version."""
    res: Optional[PrivateImageExistsResult] = execute(
        gql.gql("""
            query PrivateImageExists(
                $wsId: BigInt!
                $imageName: String!
                $version: String!
            ) {
                privateImages(
                    filter: {
                        workspaceId: { equalTo: $wsId }
                        imageName: { equalTo: $imageName }
                        version: { equalTo: $version }
                    }
                ) {
                    nodes {
                        workspaceId
                        imageName
                        version
                    }
                }
            }
        """),
        {"wsId": ws_id, "imageName": image_name, "version": version},
    )["privateImages"]

    if res is None or res["nodes"] is None:
        return False

    # match again client-side: a false positive here makes `record_in_db` skip the
    # create and report success for a record that never landed.
    # `workspaceId` comes from the `BigInt` scalar, which can serialize as a JSON
    # number; normalise to str so the comparison never silently fails to match.
    return any(
        str(node["workspaceId"]) == ws_id
        and node["imageName"] == image_name
        and node["version"] == version
        for node in res["nodes"]
    )


def record_in_db(ws_id: str, image_name: str, version: str) -> None:
    # the mutation is a plain create, so a repeat call fails on the uniqueness
    # constraint. Skipping it makes a retry of a partly completed upload safe.
    if is_recorded_in_db(ws_id, image_name, version):
        return

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


def record_in_db_or_exit(
    ws_id: str, image_name: str, version: str, *, full_image_ref: str
) -> None:
    """Record an image already in the registry, or exit with a distinct code.

    Call only after the push succeeds: a failure here means the image is published but
    unrecorded, which a caller must not treat like a failed push.
    """
    try:
        record_in_db(ws_id, image_name, version)
    # both subclass RuntimeError, so `except Exception` would relabel them as a
    # record failure and swallow the real exit code
    except (click.exceptions.Exit, click.Abort):
        raise
    except Exception as e:
        # dedent the template first: interpolating a multi-line error would leave a
        # zero-indent line, and dedent would then strip nothing from the whole block
        template = dedent("""\
            The image reached the registry, but Latch could not record it:

            {error}

            `{ref}` is pushed, but Latch has no record of it. It will not appear in
            `latch image ls`, and workflows cannot reference it.

            Re-run this command to retry the record. The record step skips itself if
            the record already exists. If the registry refuses the repeated push
            because the tag is immutable, upload under a new version instead.
        """)

        click.secho(
            template.format(error=indent(str(e), "  "), ref=full_image_ref),
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(record_failed_exit_code) from e


# note(ayush): latch register, etc. do a simplified version of this that can unnecessarily reformat
# image names
valid_image_expr = re.compile(
    r"""
    ^
        (
            [a-z0-9]+
            (
                ([_.]|__|[-]*)
                [a-z0-9]+
            )*
        )
        (
            /
            [a-z0-9]+
            (
                ([_.]|__|[-]*)
                [a-z0-9]+
            )*
        )*
    $
    """,
    re.VERBOSE,
)
valid_version_expr = re.compile(r"[\w][\w.-]{0,127}")
image_ref_expr = re.compile(
    r"^((?P<registry>[^/:]+)/)?(?P<image>[^:]+)(:(?P<version>[^:/]+))?$"
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


def resolve_pull_reference(image_ref: str) -> str:
    """Return the reference Docker pulls for `image_ref`, with the registry made explicit.

    Docker treats the first path component as a registry when it contains a `.` or a
    `:`, is `localhost`, or is not all lowercase - a repository path may not contain
    uppercase, so an uppercase component can only be a host. Anything else resolves to
    Docker Hub, so `team/tool:v1` is `docker.io/team/tool:v1` - a namespace the caller
    almost certainly does not own.
    """
    head, sep, _ = image_ref.partition("/")

    if sep == "":
        return f"docker.io/library/{image_ref}"

    if "." in head or ":" in head or head == "localhost" or head.lower() != head:
        return image_ref

    return f"docker.io/{image_ref}"


def resolve_workspace_id(workspace_id: Optional[str]) -> str:
    """Return the workspace to act on, and check that an explicit one is reachable.

    An explicit id can be a typo, so it is checked against the workspaces the user can
    access. The active workspace skips that check: it comes from the user's own config,
    and `get_workspaces` (~230ms, measured 2026-08) costs more than the `ls` query it
    would precede (~120ms). A wrong active workspace still fails, just later - at
    `get_credentials` for a push, or as an empty listing for `ls`.

    A deliberate difference from `latch register`, which checks both paths.
    """
    if workspace_id is None:
        return current_workspace()

    workspaces = get_workspaces()

    if workspace_id not in workspaces:
        click.secho(
            f"User does not have permission to access workspace {workspace_id}.",
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(1)

    click.secho(
        f"Target workspace: {workspaces[workspace_id]['name']} ({workspace_id})",
        fg="bright_blue",
    )

    return workspace_id


# note(ayush): not going to support remote here as remote instance will not have necessary
# credentials if the source image is private and i dont want to deal with federation or forwarding
# credentials
def upload_image(
    image_ref: str,
    *,
    image_name: Optional[str] = None,
    version: Optional[str] = None,
    workspace_id: Optional[str] = None,
    should_pull: bool = False,
    skip_confirmation: bool = False,
) -> None:
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
            Could not parse image version from reference `{image_ref}`

            Please either provide a human readable image reference (e.g. `registry.dockerhub.io/test_image:123`), or
            pass in a custom version using `--version`.
            """),
            fg="red",
            bold=True,
        )

        raise click.exceptions.Exit(1)

    assert image_name is not None
    assert version is not None

    ws_id = resolve_workspace_id(workspace_id)

    namespaced_image_name = f"{ws_id}_{image_name}"

    full_image_ref = f"{ecr_base}/{namespaced_image_name}:{version}"

    client = get_local_docker_client()

    # resolve the source before we prompt: the user should be confirming a known
    # source, and a missing image must not cost a credentials round trip
    pull_ref: Optional[str] = None
    try:
        client.inspect_image(image_ref)
    except docker.errors.ImageNotFound as e:
        pull_ref = resolve_pull_reference(image_ref)

        if not should_pull:
            click.secho(f"No local image matches `{image_ref}`.\n", fg="red", bold=True)

            if pull_ref != image_ref:
                click.secho(
                    dedent(f"""\
                        That reference is unqualified, so Docker resolves it to
                        `{pull_ref}` on Docker Hub. Check that you own that namespace.
                    """),
                    fg="red",
                    bold=True,
                )

            click.secho(
                f"Build the image first, or pass `--pull` to fetch `{pull_ref}`.",
                fg="red",
                bold=True,
            )

            raise click.exceptions.Exit(1) from e

    click.secho(f"Image Destination: {full_image_ref}")
    if pull_ref is not None:
        click.secho(f"Image Source: {pull_ref} (not present locally, will be pulled)")

    if not skip_confirmation and not click.confirm("Proceed?"):
        raise click.Abort

    credentials = get_credentials(namespaced_image_name, ws_id=ws_id)

    if pull_ref is not None:
        print_upload_logs(
            client.pull(pull_ref, stream=True, decode=True, platform="linux/amd64"),
            pull_ref,
            print_header=False,
        )

    client.tag(image_ref, repository=f"{ecr_base}/{namespaced_image_name}", tag=version)

    client._auth_configs = docker.auth.AuthConfig({  # noqa: SLF001
        "auths": {ecr_base: asdict(credentials)}
    })

    digest = print_upload_logs(
        client.push(
            repository=f"{ecr_base}/{namespaced_image_name}",
            tag=version,
            stream=True,
            decode=True,
            auth_config=asdict(credentials),
        ),
        namespaced_image_name,
    )

    if digest is None:
        # the push reported no error, so the tag is there - but without a digest we
        # cannot say the registry stored what we built. Say so rather than imply it.
        click.secho(
            "The registry did not report a digest, so the pushed content could not be"
            " confirmed.",
            fg="yellow",
            bold=True,
            err=True,
        )

    confirmation = "" if digest is not None else " (digest unconfirmed)"
    click.secho(f"Successfully pushed {full_image_ref}{confirmation}", fg="green")

    record_in_db_or_exit(
        ws_id, namespaced_image_name, version, full_image_ref=full_image_ref
    )


def build_and_upload_image(
    root: Path,
    *,
    image_name: str,
    version: Optional[str] = None,
    dockerfile_path: Optional[Path] = None,
    workspace_id: Optional[str] = None,
    remote: bool = True,
    skip_confirmation: bool = False,
    progress_plain: bool = False,
) -> None:
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

        version = hash_directory(root, silent=True)[:6]

    ws_id = resolve_workspace_id(workspace_id)
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
            ws_id=ws_id,
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
            ws_id=ws_id,
        )

    click.secho(f"Successfully built and tagged {full_image_ref}", fg="green")

    record_in_db_or_exit(
        ws_id, namespaced_image_name, version, full_image_ref=full_image_ref
    )


# todo(ayush): scuffed
def ls(*, workspace_id: Optional[str] = None) -> None:
    ws_id = resolve_workspace_id(workspace_id)

    res: Optional[PrivateImages] = execute(
        gql.gql(
            """
            query ListPrivateImages($wsId: BigInt!) {
                privateImages(filter: { workspaceId: { equalTo: $wsId } }) {
                    nodes {
                        imageName
                        version
                        creationTime
                    }
                }
            }
            """
        ),
        {"wsId": ws_id},
    )["privateImages"]

    # the schema types the connection as nullable but its nodes as `[PrivateImage!]!`
    # (verified by introspection), so an empty-but-readable workspace returns `nodes:
    # []`, never null. A null connection is the only null case, and it means the
    # workspace could not be read - no permission, or a partial error from the API.
    nodes = res["nodes"] if res is not None else None

    if nodes is None:
        click.secho(
            f"Could not read the private images in workspace {ws_id}.",
            fg="red",
            bold=True,
            err=True,
        )

        raise click.exceptions.Exit(1)

    if len(nodes) == 0:
        # an empty workspace is not an error. The note goes to stderr so that stdout
        # stays parseable.
        click.secho(
            f"No private images in workspace {ws_id}.", dim=True, italic=True, err=True
        )

        return

    for node in nodes:
        click.secho(f"{ecr_base}/{node['imageName']}:{node['version']}    ", nl=False)

        pretty_time = human_readable_datetime(dp.isoparse(node["creationTime"]))

        click.secho(f"created on {pretty_time}", dim=True, italic=True)
