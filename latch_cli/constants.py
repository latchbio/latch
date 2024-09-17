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


@dataclass(frozen=True)
class LatchConstants:
    base_image: str = (
        "812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:fe0b-main"
    )
    nextflow_latest_version: str = "v2.0.0"
    nextflow_latest_version: str = "v1.1.8"

    file_max_size: int = 4 * Units.MiB
    file_chunk_size: int = 64 * Units.MiB

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    maximum_upload_parts = 10000
    maximum_upload_size = 5 * Units.TiB

    pkg_name: str = "latch"
    pkg_config: str = ".latch/config"
    pkg_workflow_name: str = ".latch/workflow_name"

    # todo(aidan): make this aware of the current working directory so that we do not remove useful context
    ignore_regex = re.compile(
        r"(\.git|\.latch_report\.tar\.gz|traceback\.txt|metadata\.json)$"
    )

    jump_host: str = "jump.centromere.latch.bio"
    jump_user: str = "jumpuser"

    # seconds
    centromere_poll_timeout: int = 18000
    centromere_keepalive_interval: int = 30


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

docker_image_name_illegal_pat = re.compile(r"[^a-z0-9]+")
