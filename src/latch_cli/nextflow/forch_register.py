import json
import math
import os
import tarfile
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent
from typing import Optional, TypedDict
from urllib.parse import urljoin

import click
import requests
import tqdm

from latch.utils import current_workspace
from latch_cli.constants import latch_constants
from latch_cli.nextflow.parse_schema import parse_schema
from latch_cli.tinyrequests import post
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import NUCLEUS_URL

ignore_files = [".gitignore", ".dockerignore"]


@dataclass
class RegisterConfig:
    display_name: str
    version: str
    script_path: Path
    use_ignore_files: bool


# todo(ayush): the ignore logic seems correct on some tests still but need to properly verify
def gather_archive_content(
    parent: Path,
    archive_contents: Optional[list[Path]] = None,
    ignore_patterns: Optional[list[str]] = None,
    *,
    use_ignore_files: bool = True,
) -> list[Path]:
    if archive_contents is None:
        archive_contents = []
    if ignore_patterns is None:
        ignore_patterns = [".git"]

    if any(fnmatch(str(parent), p) for p in ignore_patterns):
        return []

    if use_ignore_files:
        for ignore_file in ignore_files:
            p = parent / ignore_file
            if not p.exists() or not p.is_file():
                continue

            ignore_patterns.extend([
                (x if parent == Path(".") else f"{parent}/{x}")
                for x in p.read_text().splitlines()
            ])

    for child in parent.iterdir():
        if any(fnmatch(str(child), p) for p in ignore_patterns):
            continue

        if child.is_dir():
            gather_archive_content(
                child,
                archive_contents,
                ignore_patterns,
                use_ignore_files=use_ignore_files,
            )
            continue

        archive_contents.append(child)

    return archive_contents


class CompletedPart(TypedDict):
    ETag: str
    PartNumber: int


def register(pkg_root: Path, *, config: RegisterConfig):
    param_spec = parse_schema(Path("nextflow_schema.json"))

    archive_contents = gather_archive_content(
        pkg_root, use_ignore_files=config.use_ignore_files
    )

    with NamedTemporaryFile() as f:
        with (
            tarfile.open(fileobj=f, mode="w:gz") as archive,
            tqdm.tqdm(
                total=len(archive_contents),
                desc="Building workflow archive",
                leave=False,
                smoothing=0,
                unit="B",
                unit_scale=True,
            ) as pbar,
        ):
            for x in archive_contents:
                archive.add(x, arcname=str(x.relative_to(pkg_root)))
                pbar.update(1)

            pbar.close()

        f.flush()
        archive_size = Path(f.name).stat().st_size
        chunk_size = latch_constants.file_chunk_size

        part_count = min(
            latch_constants.maximum_upload_parts, math.ceil(archive_size / chunk_size)
        )
        part_size = max(
            chunk_size, math.ceil(archive_size / latch_constants.maximum_upload_parts)
        )

        res = post(
            urljoin(NUCLEUS_URL, "/workflows/start-wf-upload"),
            headers={"Authorization": get_auth_header()},
            json={
                "workspace_id": int(current_workspace()),
                "workflow_display_name": config.display_name,
                "workflow_name": config.display_name,
                "version": config.version,
                "param_metadata": json.dumps({
                    "parameters": param_spec,
                    # ayush: takes advantage of the fact that python dicts preserve insertion order
                    "key_order": list(param_spec),
                }),
                "script_path": str(config.script_path),
                "part_count": part_count,
            },
        )

        if res.status_code != 200:
            click.secho(f"Unable to upload workflow files: {res.content}", fg="red")
            raise click.exceptions.Exit(1)

        data = res.json()

        upload_id = data["data"]["upload_id"]
        urls = data["data"]["urls"]
        workflow_id = data["workflow_id"]

        with tqdm.tqdm(
            total=archive_size,
            desc="Uploading workflow files",
            leave=False,
            smoothing=0,
            unit="B",
            unit_scale=True,
        ) as pbar:
            # todo(ayush): parallelize
            # todo(ayush): push through the async cp stuff and use that here
            offset: int = 0
            parts: list[CompletedPart] = []
            for idx, url in enumerate(urls):
                res = requests.put(url, data=os.pread(f.fileno(), part_size, offset))
                pbar.update(part_size)
                offset += part_size

                if res.status_code != 200:
                    click.secho(
                        f"failed to upload part {idx} of workflow files: {res.content}"
                    )
                    raise click.exceptions.Exit(1)

                etag = res.headers["ETag"]
                if etag is None:
                    click.secho(
                        f"Malformed response from chunk upload part {idx}: {res.content}"
                    )
                    raise click.exceptions.Exit(1)

                assert isinstance(etag, str)

                parts.append(
                    CompletedPart(
                        # todo(ayush): figure out why boto sometimes adds quotes to ETag values
                        ETag=etag.strip('"'),
                        PartNumber=idx + 1,
                    )
                )

        res = post(
            urljoin(NUCLEUS_URL, "/workflows/end-wf-upload"),
            headers={"Authorization": get_auth_header()},
            json={"workflow_id": workflow_id, "upload_id": upload_id, "parts": parts},
        )

        if res.status_code != 200:
            click.secho(f"Unable to upload workflow files: {res.content}", fg="red")
            raise click.exceptions.Exit(1)

        click.secho(
            dedent(f"""\
            Successfully registered workflow.
            URL: https://console.latch.bio/workflows/{workflow_id}
            """).strip(),
            fg="green",
        )
