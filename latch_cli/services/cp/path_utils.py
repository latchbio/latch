import re
import urllib.parse
from urllib.parse import urljoin, urlparse

from latch_cli.services.cp.exceptions import PathResolutionError

# todo(ayush): need a better way to check if "latch" has been appended to urllib
if urllib.parse.uses_netloc[-1] != "latch":
    urllib.parse.uses_netloc.append("latch")
    urllib.parse.uses_relative.append("latch")


latch_url_regex = re.compile(r"^(latch)?://")


def is_remote_path(path: str) -> bool:
    return latch_url_regex.match(path) is not None


def urljoins(*args: str, dir: bool = False) -> str:
    """Construct a URL by appending paths

    Paths are always joined, with extra `/`s added if missing. Does not allow
    overriding basenames as opposed to normal `urljoin`. Whether the final
    path ends in a `/` is still significant and will be preserved in the output

    >>> urljoin("latch:///directory/", "another_directory")
    latch:///directory/another_directory
    >>> # No slash means "another_directory" is treated as a filename
    >>> urljoin(urljoin("latch:///directory/", "another_directory"), "file")
    latch:///directory/file
    >>> # Unintentionally overrode the filename
    >>> urljoins("latch:///directory/", "another_directory", "file")
    latch:///directory/another_directory/file
    >>> # Joined paths as expected

    Args:
        args: Paths to join
        dir: If true, ensure the output ends with a `/`
    """

    res = args[0]
    for x in args[1:]:
        if res[-1] != "/":
            res = f"{res}/"
        res = urljoin(res, x)

    if dir and res[-1] != "/":
        res = f"{res}/"

    return res


scheme = re.compile(
    r"""
    ^(
        (latch://) |
        (file://) |
        (?P<implicit_url>://) |
        (?P<absolute_path>/) |
        (?P<relative_path>[^/])
    )
    """,
    re.VERBOSE,
)
domain = re.compile(
    r"""
    ^(
        | # empty
        (?P<account_relative>
            (\d+\.account) |
            (?P<shared>shared) |
            (shared\.\d+\.account)
        ) |
        ([^/]+\.mount) |
        (\d+\.node)
    )$
    """,
    re.VERBOSE,
)


# scheme inference rules:
#   ://domain/a/b/c => latch://domain/a/b/c
#   /a/b/c => file:///a/b/c
#   a/b/c => file:///a/b/c
def append_scheme(path: str) -> str:
    match = scheme.match(path)
    if match is None:
        raise PathResolutionError(f"{path} is not in a valid format")

    if match["implicit_url"] is not None:
        path = f"latch{path}"
    elif match["absolute_path"] is not None:
        path = f"file://{path}"
    elif match["relative_path"] is not None:
        path = f"file:///{path}"

    return path


# domain inference rules:
#   latch:///a/b/c => latch://xxx.account/a/b/c
#   latch://shared/a/b/c => latch://shared.xxx.account/a/b/c
#   latch://any_other_domain/a/b/c => unchanged
def append_domain(path: str) -> str:
    from latch_cli.config.user import user_config

    workspace = user_config.workspace_id

    parsed = urlparse(path)
    dom = parsed.netloc

    if dom == "" and workspace != "":
        dom = f"{workspace}.account"

    match = domain.match(dom)
    if match is None:
        raise PathResolutionError(f"{dom} is not a valid path domain")

    if match["shared"] is not None and workspace != "":
        dom = f"shared.{workspace}.account"

    return parsed._replace(netloc=dom).geturl()


def is_account_relative(path: str) -> bool:
    parsed = urlparse(path)
    dom = parsed.netloc

    match = domain.match(dom)
    if match is None:
        raise PathResolutionError(f"{dom} is not a valid path domain")

    return match["account_relative"] is not None


def normalize_path(path: str) -> str:
    path = append_scheme(path)

    if path.startswith("file://"):
        return path

    return append_domain(path)
