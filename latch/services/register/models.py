"""Models used in the register service."""

import os
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

        self.dkr_client = self._construct_dkr_client()
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

        # CAUTION ~ this weird formatting is maintained indepedently in the
        # nucleus endpoint and here.
        # Name for federated token request has minimum of 2 characters.
        if int(self.account_id) < 10:
            account_id = f"x{self.account_id}"
        else:
            account_id = self.account_id

        return f"{account_id}_{self.pkg_root.name}"

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

    @staticmethod
    def _construct_dkr_client():
        """Try many methods of establishing valid connection with client.

        This was helpful -
        https://github.com/docker/docker-py/blob/a48a5a9647761406d66e8271f19fab7fa0c5f582/docker/utils/utils.py#L321
        """

        def _from_env():

            host = environment.get("DOCKER_HOST")

            # empty string for cert path is the same as unset.
            cert_path = environment.get("DOCKER_CERT_PATH") or None

            # empty string for tls verify counts as "false".
            # Any value or 'unset' counts as true.
            tls_verify = environment.get("DOCKER_TLS_VERIFY")
            if tls_verify == "":
                tls_verify = False
            else:
                tls_verify = tls_verify is not None

            enable_tls = cert_path or tls_verify

            dkr_client = None
            try:
                if not enable_tls:
                    dkr_client = docker.APIClient(host)
                else:
                    if not cert_path:
                        cert_path = os.path.join(os.path.expanduser("~"), ".docker")

                    tls_config = docker.tls.TLSConfig(
                        client_cert=(
                            os.path.join(cert_path, "cert.pem"),
                            os.path.join(cert_path, "key.pem"),
                        ),
                        ca_cert=os.path.join(cert_path, "ca.pem"),
                        verify=tls_verify,
                    )
                    dkr_client = docker.APIClient(host, tls=tls_config)

            except docker.errors.DockerException as de:
                raise OSError(
                    "Unable to establish a connection to Docker. Make sure that"
                    " Docker is running and properly configured before attempting"
                    " to register a workflow."
                ) from de

            return dkr_client

        environment = os.environ

        host = environment.get("DOCKER_HOST")

        if host is not None and host != "":
            return _from_env()
        else:
            try:
                # TODO: platform specific socket defaults
                return docker.APIClient(base_url="unix://var/run/docker.sock")
            except docker.errors.DockerException as de:
                raise OSError(
                    "Docker is not running. Make sure that"
                    " Docker is running before attempting to register a workflow."
                ) from de
