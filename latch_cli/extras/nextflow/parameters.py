from pathlib import Path
from typing import Dict, List, Union

import click
from flytekit.extras.persistence import LatchPersistence
from typing_extensions import TypeAlias

from latch.types.directory import LatchDir
from latch_cli.utils import urljoins
from latch_cli.utils.ldata import LDataNodeType, get_node_data

JSONValue: TypeAlias = Union[
    int,
    str,
    bool,
    float,
    None,
    List["JSONValue"],
    Dict[str, "JSONValue"],
]


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
        res.append(Path(v))
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


def download_files(params: List[JSONValue], outdir: LatchDir):
    paths = []
    for param in params:
        _extract_paths(param, paths)

    remote = outdir.remote_path
    assert remote is not None

    remote_to_local = {urljoins(remote, local): local for local in paths}
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
        if data.type == LDataNodeType.obj:
            lp.download(remote, local)
        else:
            lp.download_directory(remote, local)

        click.echo("Done.")


def upload_files(params: List[JSONValue], outdir: LatchDir):
    paths = []
    for param in params:
        _extract_paths(param, paths)

    remote = outdir.remote_path
    assert remote is not None

    local_to_remote = {local: urljoins(remote, local) for local in paths}

    lp = LatchPersistence()
    for local in paths:
        p = Path(local).resolve()
        if not p.exists():
            click.secho(
                f"Nextflow process expects a file/directory to be at {local},"
                " but no file was found. Aborting.",
                fg="red",
            )
            raise PathNotFoundError()

        remote = local_to_remote[local]
        if p.is_file():
            lp.upload(local, remote)
        else:
            lp.upload_directory(local, remote)
