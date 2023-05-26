import re
from dataclasses import dataclass
from enum import Enum
from typing import TypedDict

import click
import gql

from latch_cli.config.user import user_config
from latch_cli.services.cp.exceptions import PathResolutionError
from latch_cli.services.cp.path_utils import (
    append_scheme,
    is_account_relative,
    normalize_path,
)
from latch_cli.services.cp.utils import get_auth_header

auth = re.compile(
    r"""
    ^(
        (?P<sdk>Latch-SDK-Token) |
        (?P<execution>Latch-Execution-Token)
    )\s.*$
""",
    re.VERBOSE,
)


class LDataNodeType(str, Enum):
    account_root = "account_root"
    dir = "dir"
    obj = "obj"
    mount = "mount"
    link = "link"


class FinalLinkTargetPayload(TypedDict):
    type: str
    name: str


class LdataNodePayload(TypedDict):
    finalLinkTarget: FinalLinkTargetPayload


class LdataResolvePathToNodePayload(TypedDict):
    path: str
    ldataNode: LdataNodePayload


class AccountInfoCurrentPayload(TypedDict):
    id: str


class GetNodeTypePayload(TypedDict):
    accountInfoCurrent: AccountInfoCurrentPayload
    ldataResolvePathToNode: LdataResolvePathToNodePayload


@dataclass(frozen=True)
class GetNodeDataResult:
    name: str
    type: LDataNodeType
    is_parent: bool


def get_node_data(
    remote_path: str, *, allow_resolve_to_parent: bool = False
) -> GetNodeDataResult:
    normalized = normalize_path(remote_path)

    from latch.gql._execute import execute

    res = execute(
        gql.gql("""
        query GetNodeType($path: String!) {
            accountInfoCurrent {
                id
            }
            ldataResolvePathToNode(path: $path) {
                path
                ldataNode {
                    finalLinkTarget {
                        name
                        type
                    }
                }
            }
        }
    """),
        {"path": normalized},
    )

    acc_info: AccountInfoCurrentPayload = res["accountInfoCurrent"]
    node: LdataResolvePathToNodePayload = res["ldataResolvePathToNode"]

    acc_id = acc_info["id"]

    try:
        final_link_target = node["ldataNode"]["finalLinkTarget"]
        remaining = node["path"]

        if final_link_target is None:
            raise ValueError("Cannot resolve path")

        is_parent = remaining is not None and remaining != ""

        if not allow_resolve_to_parent and is_parent:
            raise ValueError("Node cannot be resolved in this workspace")

        if remaining is not None and "/" in remaining:
            raise ValueError("Node cannot be resolved in this workspace")

        return GetNodeDataResult(
            name=final_link_target["name"],
            type=LDataNodeType(final_link_target["type"].lower()),
            is_parent=is_parent,
        )

    except ValueError as e:
        auth_header = get_auth_header()
        match = auth.match(auth_header)
        if match is None:
            auth_type = auth_header
        elif match["sdk"] is not None:
            auth_type = "SDK Token"
        else:
            auth_type = "Execution Token"

        auth_str = (
            f"{click.style(f'Authorized using:', bold=True, reset=False)} {click.style(auth_type, bold=False, reset=False)}"
            + "\n"
        )

        ws_id = user_config.workspace_id
        ws_name = user_config.workspace_name

        resolve_str = (
            f"{click.style(f'Relative path resolved to:', bold=True, reset=False)} {click.style(normalized, bold=False, reset=False)}"
            + "\n"
        )
        ws_str = (
            f"{click.style(f'Using Workspace:', bold=True, reset=False)} {click.style(ws_id, bold=False, reset=False)}"
        )
        if ws_name is not None:
            ws_str = f"{ws_str} ({ws_name})"
        ws_str += "\n"

        account_relative = is_account_relative(append_scheme(remote_path))

        raise PathResolutionError(
            click.style(
                f"""
{click.style(f'{remote_path}:', bold=True, reset=False)}{click.style(f" not found", bold=False, reset=False)}
{resolve_str if account_relative else ""}{ws_str if account_relative else ""}
{auth_str}
{click.style("Check that:", bold=True, reset=False)}
{click.style("1. The target object exists", bold=False, reset=False)}
{click.style(f"2. Account ", bold=False, reset=False)}{click.style(acc_id, bold=True, reset=False)}{click.style(" has permission to view the target object", bold=False, reset=False)}
{"3. The correct workspace is selected" if account_relative else ""}

For privacy reasons, non-viewable objects and non-existent objects are indistinguishable""",
                fg="red",
            )
        ) from e
