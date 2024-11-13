import os
import re
from functools import lru_cache
from pathlib import Path
from typing import List

import click
import click.shell_completion as sc

from latch_cli.services.cp.utils import (
    _get_immediate_children_of_node,
    _get_known_domains_for_account,
)

cache = lru_cache(maxsize=None)
completion_type = re.compile(
    r"""
    ^
    (latch)?
    :/?/?
    (?P<domain>[^/]*)
    (
        (?P<parent>(/[^/]*)*)?
        (?P<path>/[^/]*)
    )?
    $
    """,
    re.VERBOSE,
)


def complete(
    ctx: click.Context,
    param: click.Argument,
    incomplete: str,
    allow_local: bool = True,
) -> List[sc.CompletionItem]:
    match = completion_type.match(incomplete)

    if match is None:
        if not allow_local:
            return []

        return _complete_local_path(incomplete)
    elif match["path"] is None or len(match["path"]) == 0:
        return _complete_domain(match)
    else:
        return _complete_remote_path(match)


def remote_complete(
    ctx: click.Context,
    param: click.Argument,
    incomplete: str,
):
    return complete(ctx, param, incomplete, allow_local=False)


@cache
def _complete_local_path(incomplete: str) -> List[sc.CompletionItem]:
    # todo(maximsmol): bash needs this, zsh probably needs the real thing
    # return [sc.CompletionItem("", type="file")]

    if incomplete == "":
        parent = Path.cwd()
        stub = ""
    else:
        p = Path(incomplete).resolve()
        parent = p.parent
        stub = p.name

    res: List[sc.CompletionItem] = []
    for sub_path in parent.iterdir():
        if not sub_path.name.startswith(stub):
            continue

        rel_path = os.path.relpath(sub_path)
        typ = "file" if sub_path.is_file() else "dir"
        res.append(sc.CompletionItem(rel_path, type=typ))

    return res


@cache
def _complete_remote_path(match: re.Match) -> List[sc.CompletionItem]:
    domain = match["domain"]
    parent = match["parent"]
    path = match["path"][1:]

    parent = f"://{domain}{parent}"
    if match[0].startswith("latch"):
        parent = f"latch{parent}"

    parent_path = parent
    if not parent_path.startswith("latch"):
        parent_path = f"latch{parent_path}"

    children = _get_immediate_children_of_node(parent_path)

    res: List[sc.CompletionItem] = []
    for child in children:
        if not child.startswith(path):
            continue

        res.append(sc.CompletionItem(f"{parent}/{child}"))

    return res


@cache
def _complete_domain(match: re.Match) -> List[sc.CompletionItem]:
    stub = match["domain"]

    res: List[sc.CompletionItem] = []
    for d in _get_known_domains_for_account():
        x = f"://{d}/"
        if not d.startswith(stub):
            continue

        if match[0].startswith("latch"):
            x = f"latch{x}"
        res.append(sc.CompletionItem(x))

    return res
