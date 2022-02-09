"""Models used in the register service."""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import docker
from latch.config import LatchConfig
from latch.utils import account_id_from_token, retrieve_or_login


@dataclass
class RegisterOutput:
    """A typed structure to consolidate relevant values from the registration
    process.
    """

    build_logs: List[str] = None
    """Stdout/stderr from the container construction process."""
    serialize_logs: List[str] = None
    """Stdout/stderr from in-container serialization of workflow code."""
    registration_response: dict = None
    """JSON returned from the Latch API from a request to register serialized
    workflow code.
    """


class RegisterCtx:
    """This context object manages state for the lifetime of registration flow.

    The context holds values that are relevant throughout the "lifetime" of a
    registration, such as the location of local code and package name, as well
    as managing clients to interact with local docker servers and make remote HTTP
    requests.

    It also holds values extracted from early steps to be used later, eg. workflow
    version is parsed and stored so that it can be used in the API request for
    registration.

    Example: ::

        ctx = RegisterCtx(pkg_root)
        build_logs = _build_image(ctx, ...)
        serialize_logs = _serialize_pkg(ctx, ...)

    Args:
        pkg_root: Vaid path to root of workflow package,
        token: An optional JWT, only used for testing purposes.
    """

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
        """The image to be registered."""
        if self.account_id is None:
            raise ValueError("You need to log in before you can register a workflow.")
        return f"{self.account_id}_{self.pkg_root.name}"

    @property
    def image_tagged(self):
        """The tagged image to be registered.

        eg. dkr.ecr.us-west-2.amazonaws.com/pkg_name:version
        """

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
        """The full image to be registered.

            - Registry name
            - Repository name
            - Version/tag name

        An example: ::

            dkr.ecr.us-west-2.amazonaws.com/pkg_name:version

        """
        return f"{self.dkr_repo}/{self.image_tagged}"
