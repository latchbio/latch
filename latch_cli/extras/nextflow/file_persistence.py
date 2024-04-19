import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Union
from urllib.parse import urlparse

import click
import gql
from flytekit.extras.persistence import LatchPersistence
from latch_sdk_gql.execute import execute
from typing_extensions import TypeAlias

from latch.ldata._transfer.node import get_node_data
from latch.ldata.path import LPath
from latch.ldata.type import LDataNodeType
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
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


def _get_remote(outdir: LatchDir) -> str:
    remote = outdir.remote_path
    assert remote is not None

    exec_name = _get_execution_name()
    if exec_name is not None:
        remote = urljoins(remote, exec_name)

    return remote


@dataclass
class PathData:
    parameter: Dict[str, str]
    local: Optional[Path] = None
    remote: Optional[str] = None


def _extract_paths(parameter: JSONValue, res: List[PathData]):
    if not isinstance(parameter, Dict):
        raise ValueError(f"malformed parameter: {parameter}")

    if "path" in parameter:
        v = parameter["path"]
        assert isinstance(v, str)

        remote: Optional[str] = None
        if "remote" in parameter:
            assert isinstance(parameter["remote"], str)
            remote = parameter["remote"]

        local: Optional[Path] = None
        parsed = urlparse(v)
        if parsed.scheme == "latch":
            remote = v
        elif parsed.scheme == "":
            local = Path(v).absolute().relative_to(Path.home())
        else:
            return

        res.append(PathData(parameter=parameter, local=local, remote=remote))

    elif "list" in parameter:
        v = parameter["list"]
        assert isinstance(v, List)

        for x in v:
            _extract_paths(x, res)
    elif "map" in parameter:
        v = parameter["map"]
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


def download_files(
    channels: List[List[JSONValue]], outdir: LatchDir, *, make_impostors: bool = False
):
    path_data: List[PathData] = []
    for channel in channels:
        if type(channel) == dict and "value" in channel:
            _extract_paths(channel["value"], path_data)
        elif type(channel) == list:
            for param in channel:
                _extract_paths(param, path_data)

    remote = _get_remote(outdir)

    remote_to_local: Dict[str, Path] = {}
    for data in path_data:
        if data.local is None:
            assert data.remote is not None

            click.echo(f"Downloading {data.remote}. ", nl=False)
            p = LPath(data.remote).download()
            data.parameter["path"] = str(p)
        elif data.remote is None:
            assert data.local is not None

            remote_to_local[urljoins(remote, str(data.local))] = data.local
        else:
            remote_to_local[data.remote] = data.local

    node_data = get_node_data(*remote_to_local, allow_resolve_to_parent=True)

    downloaded: Set[str] = set()
    lp = LatchPersistence()
    for remote, data in node_data.data.items():
        local = remote_to_local[remote]

        if not data.exists():
            click.secho(
                f"Warning: Nextflow process expects a file/directory to be at {local},"
                " but no corresponding remote file was found. A previous task may not"
                " have uploaded the file.",
                fg="yellow",
            )
            continue

        if make_impostors:
            if data.type == LDataNodeType.obj:
                LatchFile(remote)._create_imposters()
            else:
                LatchDir(remote)._create_imposters()

            continue

        if remote in downloaded:
            continue

        downloaded.add(remote)

        click.echo(f"Downloading {remote}. ", nl=False)

        local.parent.mkdir(parents=True, exist_ok=True)

        if data.type == LDataNodeType.obj:
            lp.download(remote, str(local))
        else:
            lp.download_directory(remote, str(local))

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
        lp.upload(str(local), remote)
    else:
        lp.upload_directory(str(local), remote)

    click.echo("Done.")


# todo(ayush): use crc or something to avoid reuploading unchanged files
def upload_files(channels: Dict[str, List[JSONValue]], outdir: LatchDir):
    path_data: List[PathData] = []
    for channel in channels.values():
        if type(channel) == dict and "value" in channel:
            _extract_paths(channel["value"], path_data)
        elif type(channel) == list:
            for param in channel:
                _extract_paths(param, path_data)

    remote_parent = _get_remote(outdir)

    local_to_remote: Dict[Path, str] = {}
    for data in path_data:
        if data.local is None:
            continue

        remote = urljoins(remote_parent, str(data.local))
        local = Path.home() / data.local

        local_to_remote[local] = remote
        data.parameter["remote"] = remote

    published_files: List[str] = []
    try:
        with open(".latch/published.json", "r") as f:
            data = f.read()
        conf = json.loads(data)
        published_files = conf.get("files", [])
    except FileNotFoundError:
        pass

    for file in published_files:
        relative_path = Path(file).relative_to(Path.home())
        local_to_remote[file] = urljoins(remote_parent, str(relative_path))

    for local, remote in local_to_remote.items():
        _upload(local, remote)
