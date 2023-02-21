"""Package-wide constants."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class LatchConstants:
    base_image: str = (
        "812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:9c8f-main"
    )

    mib: int = 2**20

    file_max_size: int = 4 * mib

    file_chunk_size: int = 5 * mib

    pkg_name: str = "latch"
    pkg_ssh_key: str = ".latch/ssh_key"
    pkg_config: str = ".latch/config"

    # todo(aidan): make this aware of the current working directory so that we do not remove useful context
    ignore_regex = re.compile(
        r"(\.git|\.latch_report\.tar\.gz|traceback\.txt|metadata\.json)$"
    )


latch_constants = LatchConstants()


@dataclass(frozen=True)
class OAuth2Constants:

    client_id: str = "jzFBOhIbfp4EPRYZ8wmx4YyvL27LFDeB"
    """Identifies the authentication server in 0Auth2.0 flow"""

    authz_server_host: str = "https://latchai.us.auth0.com"
    """Host of the authentication server used in 0Auth2.0 flow."""

    redirect_url: str = "http://127.0.0.1:5050/callback"
    """Redirect URL registered with authentication server."""


oauth2_constants = OAuth2Constants()
