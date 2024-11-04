"""Service to list files in a remote directory."""

from dataclasses import dataclass
from datetime import datetime
from textwrap import dedent
from typing import List, Optional, TypedDict

import click
import dateutil.parser as dp
import gql
from latch_sdk_gql.execute import execute

from latch.ldata.type import LDataNodeType
from latch_cli.click_utils import bold
from latch_cli.utils import with_si_suffix
from latch_cli.utils.path import normalize_path


class _LdataObjectMeta(TypedDict):
    modifyTime: Optional[str]
    contentSize: Optional[int]


class _Child(TypedDict):
    name: str
    ldataObjectMeta: Optional[_LdataObjectMeta]
    type: str


class _Node(TypedDict):
    child: _Child


class _ChildLdataTreeEdges(TypedDict):
    nodes: List[_Node]


class _FinalLinkTarget(TypedDict):
    childLdataTreeEdges: _ChildLdataTreeEdges


class _LdataResolvePathData(TypedDict):
    name: str
    type: str
    ldataObjectMeta: Optional[_LdataObjectMeta]
    finalLinkTarget: _FinalLinkTarget


@dataclass(frozen=True)
class _Row:
    name: str
    type: LDataNodeType
    size: Optional[int]
    modify_time: Optional[datetime]


def ls(path: str, *, group_directories_first: bool = False):
    """Lists the children of a remote directory in Latch.

    Args:
        path: A valid remote path
        group_directories_first: Option to display directories/links before
            objects

    This function will list all of the entites under the remote directory
    specified in the path `path`. Will error if the path is invalid
    or the directory doesn't exist.

    Examples:
        >>> ls("")
            # Lists all entities in the user's root directory
        >>> ls("latch:///dir1/dir2/dir_name")
            # Lists all entities inside dir1/dir2/dir_name
    """
    if path == "":
        path = "/"

    normalized_path = normalize_path(path, assume_remote=True)

    query = execute(
        gql.gql("""
            query LdataInfo ($argPath: String!) {
                accountInfoCurrent {
                    id
                }
                ldataResolvePathData(argPath: $argPath) {
                    name
                    ldataObjectMeta {
                        modifyTime
                        contentSize
                    }
                    type
                    finalLinkTarget {
                        childLdataTreeEdges(filter: {
                            child: {
                                removed: { equalTo: false },
                                pending: { equalTo: false }
                            }
                        }) {
                            nodes {
                                child {
                                    name
                                    ldataObjectMeta {
                                        modifyTime
                                        contentSize
                                    }
                                    type
                                }
                            }
                        }
                    }
                }
            }
        """),
        {"argPath": normalized_path},
    )

    res: Optional[_LdataResolvePathData] = query["ldataResolvePathData"]
    acc_id: str = query["accountInfoCurrent"]["id"]

    if res is None:
        click.secho(
            dedent(f"""
            {bold(path)}: no such directory.

            Resolved to: {bold(normalized_path)}

            {bold("Check that:")}
            1. The target directory exists,
            2. Account {bold(acc_id)} has permissions to view the target directory, and
            3. The correct workspace is selected.

            For privacy reasons, non-viewable objects and non-existent objects are indistinguishable.
            """).strip("\n"),
            fg="red",
        )
        raise click.exceptions.Exit(1)

    nodes = res["finalLinkTarget"]["childLdataTreeEdges"]["nodes"]
    if LDataNodeType(res["type"].lower()) == LDataNodeType.obj:
        # ls object should just display the object's info
        nodes.append({"child": res})

    rows: List[_Row] = []
    for node in nodes:
        child = node["child"]

        meta = child["ldataObjectMeta"]
        size = modify_time = None
        if meta is not None:
            if meta["contentSize"] is not None:
                size = int(meta["contentSize"])
            if meta["modifyTime"] is not None:
                modify_time = dp.isoparse(meta["modifyTime"])

        rows.append(
            _Row(
                name=child["name"],
                type=LDataNodeType(child["type"].lower()),
                size=size,
                modify_time=modify_time,
            )
        )

    rows.sort(key=lambda row: row.name)
    if group_directories_first:
        rows.sort(key=lambda row: 1 if row.type == LDataNodeType.obj else 0)

    headers = [
        "  " + click.style("Size", underline=True),
        click.style("Date Modified", underline=True),
        click.style("Name", underline=True),
    ]

    click.echo(" ".join(headers))

    for row in rows:
        mt_str = f'{"-": <13}'
        size_str = f'{"-": >6}'
        if row.type == LDataNodeType.obj:
            if row.modify_time is not None:
                mt_str = click.style(
                    f'{row.modify_time.strftime("%d %b %H:%M"): <13}', fg="blue"
                )

            if row.size is not None:
                size_str = with_si_suffix(row.size, suffix="")
                size_str = click.style(f"{size_str: >6}", fg="bright_green")

        name_str = row.name
        if len(name_str) > 50:
            name_str = f"{name_str[:47]}..."

        if row.type != LDataNodeType.obj:
            color = "bright_blue"
            if row.type == LDataNodeType.link:
                color = "bright_magenta"

            name_str = click.style(f"{name_str}/", bold=True, fg=color)

        click.echo(f"{size_str} {mt_str} {name_str}")
