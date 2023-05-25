import re
from urllib.parse import urlparse

from latch_cli.services.cp.exceptions import PathResolutionError

is_valid_path_expr = re.compile(r"^(latch)?://")


def is_remote_path(path: str) -> bool:
    return is_valid_path_expr.match(path) is not None


def remote_joinpath(remote_path: str, other: str):
    if remote_path.endswith("/"):
        return remote_path + other
    return remote_path + "/" + other


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


# path transform rules:
#   ://domain/a/b/c => latch://domain/a/b/c
#   /a/b/c => latch:///a/b/c
#   a/b/c => latch:///a/b/c
#
# domain transform rules:
#   latch:///a/b/c => latch://xxx.account/a/b/c
#   latch://shared/a/b/c => latch://shared.xxx.account/a/b/c
#   latch://any_other_domain/a/b/c => unchanged
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
