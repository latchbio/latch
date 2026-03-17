from pathlib import Path
from textwrap import dedent
from typing import Optional

import click
import gql

from latch.utils import current_workspace
from latch_cli.docker_utils import get_default_dockerfile
from latch_sdk_gql.execute import execute

from ...centromere.ast_parsing import get_flyte_objects
from ...constants import docker_image_name_illegal_pat, latch_constants
from ...utils import WorkflowType, hash_directory, identifier_suffix_from_str
from ..docker.utils import dbnp, get_local_docker_client, remote_dbnp


def register_staging(
    pkg_root: Path,
    *,
    disable_auto_version: bool = False,
    disable_git_version: bool = False,
    remote: bool = False,
    skip_confirmation: bool = False,
    wf_module: Optional[str] = None,
    progress_plain: bool = False,
    dockerfile_path: Optional[Path] = None,
):
    wf_module = wf_module if wf_module is not None else "wf"
    module_path = pkg_root / Path(wf_module.replace(".", "/"))

    if dockerfile_path is None:
        dockerfile_path = get_default_dockerfile(
            pkg_root, wf_type=WorkflowType.latchbiosdk
        )

    try:
        flyte_objects = get_flyte_objects(module_path)
    except ModuleNotFoundError as e:
        click.secho(
            dedent(
                f"""
                Unable to locate workflow module `{wf_module}` in `{pkg_root.resolve()}`. Check that:

                1. {module_path} exists.
                2. Package `{wf_module}` is an absolute importable Python path (e.g. `workflows.my_workflow`).
                3. All directories in `{module_path}` contain an `__init__.py` file."""
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    wf_name: Optional[str] = None

    name_path = pkg_root / latch_constants.pkg_workflow_name
    if name_path.exists():
        click.echo(f"Parsing workflow name from {name_path}.")
        wf_name = name_path.read_text().strip()

    if wf_name is None:
        click.echo(f"Searching {module_path} for @workflow function.")
        for obj in flyte_objects:
            if obj.type != "workflow":
                continue

            wf_name = obj.name
            break

    if wf_name is None:
        click.secho(
            dedent(f"""
            Unable to find a function decorated with `@workflow` in {module_path}. Please double check that
            the value of `--workflow-module` is correct.
            """),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    version_file = pkg_root / "version"
    try:
        version_base = version_file.read_text().strip()
    except OSError:
        if not skip_confirmation and not click.confirm(
            "Could not find a `version` file in the package root. One will be created. Proceed?"
        ):
            return

        version_base = "0.1.0"
        version_file.write_text(version_base)
        click.echo(f"Created a version file with initial version {version_base}.")

    components: list[str] = [version_base, "staging"]

    if disable_auto_version:
        click.echo("Skipping version tagging due to `--disable-auto-version`")
    elif disable_git_version:
        click.echo("Skipping git version tagging due to `--disable-git-version`")

    if not disable_auto_version and not disable_git_version:
        try:
            from git import GitError, Repo

            try:
                repo = Repo(pkg_root)
                sha = repo.head.commit.hexsha[:6]
                components.append(sha)
                click.echo(f"Tagging version with git commit {sha}.")
                click.secho(
                    "  Disable with --disable-git-version/-G", dim=True, italic=True
                )

                if repo.is_dirty():
                    components.append("wip")
                    click.secho(
                        "  Repo contains uncommitted changes - tagging version with `wip`",
                        italic=True,
                    )
            except GitError:
                pass
        except ImportError:
            pass

    if not disable_auto_version:
        sha = hash_directory(pkg_root, silent=True)[:6]
        components.append(sha)
        click.echo(f"Tagging version with directory checksum {sha}.")
        click.secho("  Disable with --disable-auto-version/-d", dim=True, italic=True)

    version = "-".join(components)

    click.echo()

    res = execute(
        gql.gql("""
        query LatestVersion($wsId: BigInt!, $name: String!, $version: String!) {
            latchDevelopStagingImages(
                filter: {
                    ownerId: { equalTo: $wsId }
                    workflowName: { equalTo: $name }
                    version: { equalTo: $version }
                }
            ) {
                totalCount
            }
        }
        """),
        {"wsId": current_workspace(), "name": wf_name, "version": version},
    )

    if res["latchDevelopStagingImages"]["totalCount"] != 0:
        click.secho(
            f"Version `{version}` already exists for workflow `{wf_name}` in workspace `{current_workspace()}`. ",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if not skip_confirmation:
        if not click.confirm("Start registration?"):
            click.secho("Cancelled", bold=True)
            return
    else:
        click.secho("Skipping confirmation because of --yes", bold=True)

    image_suffix = docker_image_name_illegal_pat.sub(
        "_", identifier_suffix_from_str(wf_name).lower()
    )
    image_prefix = current_workspace()
    if len(image_prefix) == 1:
        # note(ayush): the sins of our past continue to haunt us
        image_prefix = f"x{image_prefix}"

    image = f"{image_prefix}_{image_suffix}"

    if remote:
        remote_dbnp(
            pkg_root, image, version, dockerfile_path, progress_plain=progress_plain
        )
    else:
        client = get_local_docker_client()

        dbnp(
            client,
            pkg_root,
            image,
            version,
            dockerfile_path,
            progress_plain=progress_plain,
        )

    execute(
        gql.gql("""
        mutation AddStagingImage(
            $wsId: BigInt!
            $workflowName: String!
            $version: String!
        ) {
            createLatchDevelopStagingImage(
                input: {
                    latchDevelopStagingImage: {
                        ownerId: $wsId
                        workflowName: $workflowName
                        version: $version
                    }
                }
            ) {
                clientMutationId
            }
        }
        """),
        {"wsId": current_workspace(), "workflowName": wf_name, "version": version},
    )

    click.secho("Successfully staged workflow.", fg="green")
