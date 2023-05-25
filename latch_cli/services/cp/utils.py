import os
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, TypedDict
from urllib.parse import urlparse

import gql

from latch_cli.config.user import user_config
from latch_cli.services.cp.exceptions import AuthenticationError, PathResolutionError


def get_auth_header() -> Dict[str, str]:
    sdk_token = user_config.token
    execution_token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")

    if sdk_token is not None and sdk_token != "":
        headers = {"Authorization": f"Latch-SDK-Token {sdk_token}"}
    elif execution_token is not None:
        headers = {"Authorization": f"Latch-Execution-Token {execution_token}"}
    else:
        raise AuthenticationError("Unable to find authentication credentials.")

    return headers


# path transform rules:
#   ://domain/a/b/c => latch://domain/a/b/c
#   /a/b/c => latch:///a/b/c
#   a/b/c => latch:///a/b/c
#
# domain transform rules:
#   latch:///a/b/c => latch://xxx.account/a/b/c
#   latch://shared/a/b/c => latch://shared.xxx.account/a/b/c
#   latch://any_other_domain/a/b/c => unchanged

is_valid_path_expr = re.compile(r"^(latch)?://")


def is_remote_path(path: str) -> bool:
    return is_valid_path_expr.match(path) is not None


legacy_expr = re.compile(r"^(account_root|mount)/([^/]+)(?:/+(.*))?$")
scheme_expr = re.compile(
    r"^(?:"
    r"(?P<full>latch://[^/]*(/+.*)?)"
    r"|(?P<missing_latch>://[^/]*(/+.*)?)"
    r"|(?P<missing_scheme_with_leading_slash>/+.*)"
    r"|(?P<missing_scheme_without_leading_slash>[^/]+.*)"
    r")$"
)
domain_expr = re.compile(
    r"^(((shared\.)?\d+\.account)"  # shared.{acc_id}.account
    r"|((.+)\.mount)"  # {bucket}.mount
    r"|(archive)"  # archive
    r"|((?P<shared_without_selector>shared))"  # shared
    r"|(\d+\.node))$"  # {node_id}.node
)


def normalize_path(path: str) -> str:
    if legacy_expr.match(path):
        return path  # let nuke-data deal with legacy paths

    match = scheme_expr.match(path)
    if match is None:
        raise PathResolutionError(f"{path} is not in a valid format")

    if match["missing_latch"] is not None:
        path = f"latch{path}"
    elif match["missing_scheme_with_leading_slash"] is not None:
        path = f"latch://{path}"
    elif match["missing_scheme_without_leading_slash"] is not None:
        path = f"latch:///{path}"

    from latch_cli.config.user import user_config

    workspace = user_config.workspace

    parsed = urlparse(path)
    domain = parsed.netloc

    if domain == "" and workspace != "":
        domain = f"{workspace}.account"

    match = domain_expr.match(domain)
    if match is None:
        raise PathResolutionError(f"{domain} is not a valid path domain")

    if match["shared_without_selector"] is not None and workspace != "":
        domain = f"shared.{workspace}.account"

    return parsed._replace(netloc=domain).geturl()


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


@dataclass(frozen=True)
class GetNodeDataResult:
    name: str
    type: LDataNodeType
    is_parent: bool


def get_node_data(
    remote_path: str, *, allow_resolve_to_parent: bool = False
) -> GetNodeDataResult:
    from latch.gql._execute import execute

    res: LdataResolvePathToNodePayload = execute(
        gql.gql("""
        query GetNodeType($path: String!) {
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
        {"path": remote_path},
    )["ldataResolvePathToNode"]

    try:
        final_link_target = res["ldataNode"]["finalLinkTarget"]
        remaining = res["path"]

        if not allow_resolve_to_parent and remaining is not None and remaining != "":
            raise ValueError("Node cannot be resolved in this workspace")

        is_parent = remaining is not None and remaining != ""

        if remaining is not None and "/" in remaining:
            raise ValueError("Node cannot be resolved in this workspace")

        return GetNodeDataResult(
            name=final_link_target["name"],
            type=LDataNodeType(final_link_target["type"].lower()),
            is_parent=is_parent,
        )
    except (TypeError, ValueError) as e:
        raise PathResolutionError(
            f"Cannot resolve {remote_path} in this workspace. Ensure that you are in"
            " the correct workspace, that the node exists, and that you have correct"
            " permissions."
        ) from e


def pluralize(singular: str, plural: str, selector: int) -> str:
    if selector == 1:
        return singular
    return plural


def remote_joinpath(remote_path: str, other: str):
    if remote_path.endswith("/"):
        return remote_path + other
    return remote_path + "/" + other


def human_readable_time(t_seconds: float) -> str:
    s = t_seconds % 60
    m = (t_seconds // 60) % 60
    h = t_seconds // 60 // 60

    x = []
    if h > 0:
        x.append(f"{int(h):d}h")
    if m > 0:
        x.append(f"{int(m):d}m")
    if s > 0:
        x.append(f"{s:.2f}s")

    return " ".join(x)
