"""
register.models
~~~~~
Registers workflows with the latch platform.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import docker
from latch.config import LatchConfig, UserConfig
from latch.services import login
from latch.utils import sub_from_jwt


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
    latch_register_api_url = "https://nucleus.ligma.ai/api/register-workflow"

    def __init__(self, pkg_root: Path):

        try:
            self.dkr_client = docker.APIClient(base_url="unix://var/run/docker.sock")

        except docker.errors.DockerException:
            raise OSError(
                "Docker is not running. Make sure that"
                " Docker is running before attempting to register a workflow."
            )

        self.pkg_root = pkg_root
        self.dkr_repo = LatchConfig.dkr_repo

        user_conf = UserConfig()
        token = user_conf.token
        if token is None:
            login()
            token = user_conf.token

        self.token = token
        self.user_sub = sub_from_jwt(token)

    @property
    def image(self):
        if self.user_sub is None or self.pkg_name is None:
            raise ValueError(
                "Attempting to create an image name without first"
                " logging in or extracting the package name."
            )
        fmt_sub = self.user_sub.replace("|", "-")
        return f"{fmt_sub}_{self.pkg_name}"

    @property
    def image_tagged(self):
        if self.image is None or self.version is None:
            raise ValueError(
                "Attempting to create a tagged image name without first"
                " logging in or extracting the package version."
            )
        return f"{self.dkr_repo}/{self.image}:{self.version}"
