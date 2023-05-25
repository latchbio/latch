import os
from typing import Dict

from latch_cli.config.user import user_config
from latch_cli.services.cp.exceptions import AuthenticationError


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


def pluralize(singular: str, plural: str, selector: int) -> str:
    if selector == 1:
        return singular
    return plural


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
