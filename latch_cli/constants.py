"""Package-wide constants."""

import re
from dataclasses import dataclass
from enum import Enum


class Units(int, Enum):
    KiB = 2**10
    kB = 10**3

    MiB = 2**20
    MB = 10**6

    GiB = 2**30
    GB = 10**9

    TiB = 2**40
    TB = 10**12


units = Units


@dataclass(frozen=True)
class LatchConstants:
    base_image: str = (
        "812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:5303-main"
    )

    file_max_size: int = 4 * units.MiB
    file_chunk_size: int = 256 * units.MiB

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    maximum_upload_parts = 10000
    maximum_upload_size = 5 * units.TiB

    pkg_name: str = "latch"
    pkg_ssh_key: str = ".latch/ssh_key"
    pkg_jump_key: str = ".latch/jump_key"
    pkg_config: str = ".latch/config"

    # todo(aidan): make this aware of the current working directory so that we do not remove useful context
    ignore_regex = re.compile(
        r"(\.git|\.latch_report\.tar\.gz|traceback\.txt|metadata\.json)$"
    )

    # todo(ayush): add a dns record so this isn't hot garbage
    jump_host = (
        "a379501a3e5e54a2c8d1cc4f7ed32630-1582965659.us-west-2.elb.amazonaws.com"
    )
    jump_user = "jumpuser"


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
