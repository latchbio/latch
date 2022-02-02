"""
register.models
~~~~~
Registers workflows with the latch platform.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import docker
from latch.config import LatchConfig
from latch.utils import account_id_from_token, retrieve_or_login


@dataclass
class RegisterOutput:
    build_logs: List[str] = None
    serialize_logs: List[str] = None
    registration_response: dict = None


class RegisterCtx:
    dkr_repo: Optional[str] = None
    dkr_client: docker.APIClient = None
    pkg_name: str = None
    pkg_root: Path = None  # root
    image_full = None
    token = None
    version = None
    serialize_dir = None
    latch_register_api_url = "https://nucleus.latch.bio/sdk/register-workflow"
    latch_image_api_url = "https://nucleus.latch.bio/sdk/initiate-image-upload"

    def __init__(self, pkg_root: Path):

        try:
            self.dkr_client = docker.APIClient(base_url="unix://var/run/docker.sock")

        except docker.errors.DockerException:
            raise OSError(
                "Docker is not running. Make sure that"
                " Docker is running before attempting to register a workflow."
            )

        self.pkg_root = Path(pkg_root).resolve()
        self.dkr_repo = LatchConfig.dkr_repo

        self.token = retrieve_or_login()
        self.account_id = account_id_from_token(self.token)

    @property
    def image(self):
        if self.account_id is None:
            raise ValueError("You need to log in before you can register a workflow.")
        if self.pkg_name is None:
            raise ValueError(
                "Attempting to register a workflow before the package name is known - something is wrong."
            )
        return f"{self.account_id}_{self.pkg_name}"

    @property
    def image_tagged(self):
        # TODO (kenny): check version is valid for docker
        if self.version is None:
            raise ValueError(
                "Attempting to create a tagged image name without first"
                "extracting the package version."
            )
        if self.image is None or self.version is None:
            raise ValueError(
                "Attempting to create a tagged image name without first"
                " logging in or extracting the package version."
            )
        return f"{self.image}:{self.version}"

    @property
    def full_image_tagged(self):
        return f"{self.dkr_repo}/{self.image_tagged}"
