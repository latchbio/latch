import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Union

import click
import gql
from flytekit.extras.persistence import LatchPersistence
from latch_sdk_gql.execute import execute
from typing_extensions import TypeAlias

from latch.ldata._transfer.node import get_node_data
from latch.ldata.type import LDataNodeType
from latch.types.directory import LatchDir
from latch_cli.utils import urljoins

JSONValue: TypeAlias = Union[
    int,
    str,
    bool,
    float,
    None,
    List["JSONValue"],
    Dict[str, "JSONValue"],
]


lp = LatchPersistence()


class PathNotFoundError(RuntimeError): ...


def _extract_paths(parameter: JSONValue, res: List[Path]):
    if not isinstance(parameter, Dict):
        raise ValueError(f"malformed parameter: {parameter}")

    if len(parameter.keys()) != 1:
        raise ValueError(
            f"malformed parameter does not have exactly one key: {parameter}"
        )

    k = next(iter(parameter.keys()))
    v = parameter[k]

    if k == "path":
        assert isinstance(v, str)

        res.append(Path(v).resolve())
    elif k == "list":
        assert isinstance(v, List)

        for x in v:
            _extract_paths(x, res)
    elif k == "map":
        assert isinstance(v, List)

        for x in v:
            assert isinstance(x, Dict)
            if "key" not in x or "value" not in x:
                raise ValueError(f"malformed map entry: {x}")

            _extract_paths(x["key"], res)
            _extract_paths(x["value"], res)


def _get_execution_name() -> Optional[str]:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID", None)
    if token is None:
        return None

    res = execute(
        gql.gql("""
        query executionCreatorsByToken($token: String!) {
            executionCreatorByToken(token: $token) {
                flytedbId
                info {
                    displayName
                }
            }
        }
        """),
        {"token": token},
    )["executionCreatorByToken"]

    return res["info"]["displayName"]


def download_files(channels: List[List[JSONValue]], outdir: LatchDir):
    paths: List[Path] = []
    for channel in channels:
        for param in channel:
            _extract_paths(param, paths)

    remote = outdir.remote_path
    assert remote is not None

    exec_name = _get_execution_name()
    if exec_name is not None:
        remote = urljoins(remote, exec_name)

    remote_to_local = {urljoins(remote, str(local)[1:]): local for local in paths}
    node_data = get_node_data(*remote_to_local, allow_resolve_to_parent=True)

    lp = LatchPersistence()
    for remote, data in node_data.data.items():
        local = remote_to_local[remote]

        if data.is_parent:
            click.secho(
                f"Nextflow process expects a file/directory to be at {local}, but no"
                " corresponding remote file was found. A previous task may not have"
                " uploaded the file. Aborting.",
                fg="red",
            )
            raise PathNotFoundError()

        click.echo(f"Downloading {remote} -> {local}. ", nl=False)

        local.parent.mkdir(parents=True, exist_ok=True)

        if data.type == LDataNodeType.obj:
            lp.download(remote, local)
        else:
            lp.download_directory(remote, local)

        click.echo("Done.")


def _upload(local: Path, remote: str):
    p = Path(local).resolve()
    if not p.exists():
        click.secho(
            f"Nextflow process expects a file/directory to be at {local},"
            " but no file was found. Aborting.",
            fg="red",
        )
        raise PathNotFoundError()

    click.echo(f"Uploading {local} -> {remote}. ", nl=False)

    if p.is_file():
        lp.upload(local, remote)
    else:
        lp.upload_directory(local, remote)

    click.echo("Done.")


def upload_files(channels: Dict[str, List[JSONValue]], outdir: LatchDir):
    paths: List[Path] = []
    for channel in channels.values():
        for param in channel:
            _extract_paths(param, paths)

    remote = outdir.remote_path
    assert remote is not None

    exec_name = _get_execution_name()
    if exec_name is not None:
        remote = urljoins(remote, exec_name)

    local_to_remote = {local: urljoins(remote, str(local)[1:]) for local in paths}

    for local in paths:
        _upload(local, local_to_remote[local])


def stage_for_output(channels: List[List[JSONValue]], outdir: LatchDir):
    download_files(channels, outdir)

    old: List[Path] = []
    for channel in channels:
        for param in channel:
            _extract_paths(param, old)

    remote = outdir.remote_path
    assert remote is not None

    exec_name = _get_execution_name()
    if exec_name is not None:
        remote = urljoins(remote, exec_name)

    for local_path in old:
        new_path = Path("/root/outputs") / local_path.name
        os.renames(local_path, new_path)
        _upload(new_path, urljoins(remote, "output", new_path.name))
