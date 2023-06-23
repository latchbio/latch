try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import os
import re
from pathlib import Path
from typing import List, TypedDict

import click
import click.shell_completion as sc
import gql
from latch_sdk_gql.execute import execute

from latch_cli.services.cp.ldata_utils import (
    _get_immediate_children_of_node,
    _get_known_domains_for_account,
)
from latch_cli.services.cp.path_utils import urljoins

completion_type = re.compile(
    r"""
    ^(latch)? ://(
        (?P<domain>[^/]*)
        | (?P<remote_path>[^/]*/.*)
    )$
    """,
    re.VERBOSE,
)


def complete(
    ctx: click.Context,
    param: click.Argument,
    incomplete: str,
) -> List[sc.CompletionItem]:
    match = completion_type.match(incomplete)

    if match is None:
        return _complete_local_path(incomplete)
    elif match["domain"] is not None:
        return _complete_domain(incomplete)
    else:
        return _complete_remote_path(incomplete)


def remote_complete(
    ctx: click.Context,
    param: click.Argument,
    incomplete: str,
):
    match = completion_type.match(incomplete)

    if match is None:
        return []
    elif match["domain"] is not None:
        return _complete_domain(incomplete)
    else:
        return _complete_remote_path(incomplete)


@cache
def _complete_local_path(incomplete: str) -> List[sc.CompletionItem]:
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


# `incomplete` assumed to be of the form '(latch)?://[DOMAIN]/.*'
@cache
def _complete_remote_path(incomplete: str) -> List[sc.CompletionItem]:
    parent, stub = tuple(incomplete.rsplit("/", 1))
    children = _get_immediate_children_of_node(parent)

    res: List[sc.CompletionItem] = []
    for child in children:
        if child.startswith(stub):
            res.append(sc.CompletionItem(urljoins(parent, child)))

    return res


domain = re.compile(r"^(latch)?://(?P<stub>[^/]*)$")


# `incomplete` assumed to be of the form '(latch)?://[^/]*'
@cache
def _complete_domain(incomplete: str) -> List[sc.CompletionItem]:
    match = domain.match(incomplete)
    if match is None:
        return []

    stub = match["stub"]

    res: List[sc.CompletionItem] = []
    for d in _get_known_domains_for_account():
        if d.startswith(stub):
            res.append(sc.CompletionItem(f"latch://{d}/"))

    return res
