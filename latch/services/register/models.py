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
    pkg_root: Path = None  # root
    image_full = None
    token = None
    version = None
    serialize_dir = None
    latch_register_api_url = "https://nucleus.latch.bio/sdk/register-workflow"
    latch_image_api_url = "https://nucleus.latch.bio/sdk/initiate-image-upload"

    def __init__(self, pkg_root: Path, token: Optional[str] = None):
        """
        Args:
            pkg_root: Root of workflow pkg, which contains at minimum an
                __init__.py + version file.
            token: Manually pass a JWT for the registration ctx - used for
                testing
        """

        try:
            self.dkr_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        except docker.errors.DockerException as de:
            raise OSError(
                "Docker is not running. Make sure that"
                " Docker is running before attempting to register a workflow."
            ) from de

        self.pkg_root = Path(pkg_root).resolve()
        try:
            version_file = self.pkg_root.joinpath("version")
            with open(version_file, "r") as vf:
                self.version = vf.read().strip()
        except Exception as e:
            raise ValueError(
                "Unable to extract pkg version from" f" {str(self.pkg_root)}"
            ) from e

        self.dkr_repo = LatchConfig.dkr_repo

        if token is None:
            self.token = retrieve_or_login()
        else:
            self.token = token

        self.account_id = account_id_from_token(self.token)

    @property
    def image(self):
        if self.account_id is None:
            raise ValueError("You need to log in before you can register a workflow.")
        return f"{self.account_id}_{self.pkg_root.name}"

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
