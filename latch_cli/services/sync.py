import os
import stat
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click
import dateutil.parser as dp
import gql
from gql.transport.exceptions import TransportQueryError
from latch_sdk_gql.execute import JsonValue, execute

import latch.ldata._transfer.upload as _upl
from latch.ldata._transfer.utils import get_max_workers
from latch_cli.utils.path import is_remote_path, normalize_path


def upload_file(src: Path, dest: str):
    start = _upl.start_upload(src, dest)
    if start is None:
        return

    parts: List[_upl.CompletedPart] = []
    for idx, url in enumerate(start.urls):
        parts.append(
            _upl.upload_file_chunk(
                src,
                url,
                idx,
                start.part_size,
            )
        )

    _upl.end_upload(dest, start.upload_id, parts)


def check_src(p: Path, *, indent: str = "") -> Optional[Tuple[Path, os.stat_result]]:
    try:
        p_stat = os.stat(p)
    except FileNotFoundError:
        click.secho(indent + f"`{p}`: no such file or directory", fg="red", bold=True)
        return

    if not stat.S_ISREG(p_stat.st_mode) and not stat.S_ISDIR(p_stat.st_mode):
        click.secho(indent + f"`{p}`: not a regular file", fg="red", bold=True)
        return

    return (p, p_stat)


def sync_rec(
    srcs: Dict[str, Tuple[Path, os.stat_result]],
    dest: str,
    *,
    delete: bool,
    level: int = 0,
    executor: ProcessPoolExecutor,
):
    # rsync never deletes from the top level destination
    delete_effective = delete and level > 0
    indent = "  " * level

    try:
        query = """
            query LatchCLISync($argPath: String! ${name_filter_arg}) {
                ldataResolvePathData(argPath: $argPath) {
                    finalLinkTarget {
                        type
                        childLdataTreeEdges(
                            filter: {
                                child: {
                                    removed: {equalTo: false},
                                    pending: {equalTo: false},
                                    copiedFrom: {isNull: true}
                                    ${name_filter}
                                }
                            }
                        ) {
                            nodes {
                                child {
                                    id
                                    name
                                    finalLinkTarget {
                                        type
                                        ldataNodeEvents(
                                            condition: {type: INGRESS},
                                            orderBy: TIME_DESC,
                                            first: 1
                                        ) {
                                            nodes {
                                                time
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """

        args: JsonValue = {"argPath": dest, "nameFilter": []}
        if not delete_effective:
            query = query.replace("${name_filter_arg}", ", $nameFilter: [String!]")
            query = query.replace("${name_filter}", ", name: {in: $nameFilter}")
            args["nameFilter"] = list(srcs.keys())
        else:
            query = query.replace("${name_filter_arg}", "")
            query = query.replace("${name_filter}", "")

        resolve_data = execute(
            gql.gql(query),
            args,
        )["ldataResolvePathData"]

        dest_data = None
        if resolve_data is not None:
            dest_data = resolve_data["finalLinkTarget"]
    except TransportQueryError as e:
        if e.errors is None or len(e.errors) == 0:
            raise

        msg: str = e.errors[0]["message"]

        raise

    if len(srcs) == 0:
        if dest_data is not None:
            if dest_data["type"] != "DIR":
                click.secho(
                    indent + f"`{dest}` is in the way of a directory",
                    fg="red",
                )
                return

            click.secho(indent + "Empty directory", dim=True)
            return

        if not dest[-1] == "/":
            dest += "/"

        click.secho(indent + "Creating empty directory", fg="bright_blue")
        execute(
            gql.gql("""
                mutation LatchCLISyncMkdir($argPath: String!) {
                    ldataMkdirp(input: {argPath: $argPath}) {
                        clientMutationId
                    }
                }
            """),
            {"argPath": dest},
        )
        return

    if (
        (len(srcs) > 1 or stat.S_ISDIR(list(srcs.values())[0][1].st_mode))
        and dest_data is not None
        and dest_data["type"] not in {"DIR", "ACCOUNT_ROOT"}
    ):
        click.secho(f"`{dest}` is not a directory", fg="red", bold=True)
        click.secho("\nOnly a single file can be synced with a file", fg="red")
        sys.exit(1)

    if dest_data is not None and dest_data["type"] not in {"DIR", "ACCOUNT_ROOT"}:
        # todo(maximsmol): implement
        click.secho(
            "Syncing single files is currently not supported", bold=True, fg="red"
        )
        sys.exit(1)

    dest_children_by_name = (
        {
            x["name"]: x
            for x in (raw["child"] for raw in dest_data["childLdataTreeEdges"]["nodes"] if raw["child"])
        }
        if dest_data is not None
        else {}
    )

    for name, (p, p_stat) in srcs.items():
        is_dir = stat.S_ISDIR(p_stat.st_mode)

        child = dest_children_by_name.get(name)

        if dest[-1] == "/":
            child_dest = f"{dest}{name}"
        else:
            child_dest = f"{dest}/{name}"

        skip = False
        verb = "Uploading"
        reason = "new"
        if child is not None:
            flt = child["finalLinkTarget"]
            if flt["type"] == "DIR" and not is_dir:
                # todo(maximsmol): confirm? pre-check?
                click.secho(
                    indent + f"`{dest}` is in the way of a file",
                    fg="red",
                )
                continue

            if flt["type"] != "DIR" and is_dir:
                # todo(maximsmol): confirm? pre-check?
                click.secho(
                    indent + f"`{dest}` is in the way of a directory",
                    fg="red",
                )
                continue

            if flt["type"] == "OBJ":
                remote_mtime = None
                if len(flt["ldataNodeEvents"]["nodes"]) > 0:
                    remote_mtime = dp.isoparse(flt["ldataNodeEvents"]["nodes"][0]["time"])

                local_mtime = datetime.fromtimestamp(p_stat.st_mtime).astimezone()
                if remote_mtime is not None and remote_mtime == local_mtime:
                    verb = "Skipping"
                    reason = "unmodified"
                    skip = True
                elif remote_mtime is not None and remote_mtime > local_mtime:
                    verb = "Skipping"
                    reason = "older"
                    skip = True
                else:
                    verb = "Uploading"
                    reason = "updated"
            else:
                reason = "existing"

        if verb == "Uploading" and is_dir:
            verb = "Syncing"

        fg = "bright_blue"
        dim = None
        if verb == "Skipping":
            fg = None
            dim = True

        click.echo(
            click.style(
                indent + verb + " ",
                fg=fg,
                dim=dim,
            )
            + click.style(
                reason,
                underline=True,
                fg=fg,
                dim=dim,
            )
            + click.style(
                ": ",
                fg=fg,
                dim=dim,
            )
            + click.style(
                str(p)
                + ("" if not is_dir else "/")
                + ("" if skip else click.style(" -> ", dim=True) + child_dest),
                dim=dim,
            )
        )
        if skip:
            continue

        if is_dir:
            sub_srcs: Dict[str, Tuple[Path, os.stat_result]] = {}
            for x in p.iterdir():
                res = check_src(x, indent=indent + "  ")
                if res is None:
                    # todo(maximsmol): pre-check or confirm?
                    continue

                sub_srcs[x.name] = res
            sync_rec(sub_srcs, child_dest, delete=delete, level=level + 1, executor=executor)
            continue

        executor.submit(upload_file, p, child_dest)

    if delete_effective:
        for name, child in dest_children_by_name.items():
            child_dest = f"{dest}/{name}"
            if name in srcs:
                continue

            click.echo(
                indent + click.style("Removing extraneous: ", fg="yellow") + child_dest
            )
            execute(
                gql.gql("""
                    mutation LatchCLISyncRemove($argNodeId: BigInt!) {
                        ldataRmr(input: {argNodeId: $argNodeId}) {
                            clientMutationId
                        }
                    }
                """),
                {"argNodeId": child["id"]},
            )


def sync(
    srcs_raw: List[str],
    dest: str,
    *,
    delete: bool,
    ignore_unsyncable: bool,
    cores: Optional[int] = None,
):
    if not is_remote_path(dest):
        click.secho(
            f"`{dest}`: only local -> remote sync is supported", fg="red", bold=True
        )
        raise click.exceptions.Exit(1)

    srcs: Dict[str, Tuple[Path, os.stat_result]] = {}
    have_errors = False
    for x in srcs_raw:
        if is_remote_path(x):
            click.secho(
                f"`{x}`: only local -> remote sync is supported", fg="red", bold=True
            )
            have_errors = True
            continue

        p = Path(x)
        res = check_src(p)
        if res is None:
            have_errors = True
            continue

        srcs[p.name] = res

    if len(srcs) == 0:
        click.secho(
            "\nAll source paths were skipped due to errors", fg="red", bold=True
        )
        raise click.exceptions.Exit(1)

    if have_errors:
        # todo(maximsmol): do we want to precheck recursively?
        click.secho("\nSome source paths will be skipped due to errors", fg="red")

        if not ignore_unsyncable:
            if not click.confirm(click.style(f"Proceed?", fg="red")):
                sys.exit(1)
        else:
            click.secho(
                "Proceeding due to " + click.style("`--ignore-unsyncable`", bold=True),
                fg="yellow",
            )
        click.echo()

    if cores is None:
        cores = get_max_workers()

    with ProcessPoolExecutor(max_workers=cores) as executor:
        sync_rec(srcs, normalize_path(dest), delete=delete, executor=executor)
        executor.shutdown(wait=True)
