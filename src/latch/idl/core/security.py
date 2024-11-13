from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import flyteidl.core.security_pb2 as pb

from ..utils import to_idl_many, try_to_idl


@dataclass
class Secret:
    """
    Secret encapsulates information about the secret a task needs to proceed. An environment variable
    FLYTE_SECRETS_ENV_PREFIX will be passed to indicate the prefix of the environment variables that will be present if
    secrets are passed through environment variables.
    FLYTE_SECRETS_DEFAULT_DIR will be passed to indicate the prefix of the path where secrets will be mounted if secrets
    are passed through file mounts.
    """

    class MountType(int, Enum):
        any = pb.Secret.ANY
        """Default case, indicates the client can tolerate either mounting options."""

        env_var = pb.Secret.ENV_VAR
        """ENV_VAR indicates the secret needs to be mounted as an environment variable."""

        file = pb.Secret.FILE
        """FILE indicates the secret needs to be mounted as a file."""

        def to_idl(self) -> pb.Secret.MountType:
            return self.value

    group: str
    """
    The name of the secret group where to find the key referenced below. For K8s secrets, this should be the name of
    the v1/secret object. For Confidant, this should be the Credential name. For Vault, this should be the secret name.
    For AWS Secret Manager, this should be the name of the secret.
    +required
    """

    group_version: Optional[str] = None
    """
    The group version to fetch. This is not supported in all secret management systems. It'll be ignored for the ones
    that do not support it.
    +optional
    """

    key: Optional[str] = None
    """
    The name of the secret to mount. This has to match an existing secret in the system. It's up to the implementation
    of the secret management system to require case sensitivity. For K8s secrets, Confidant and Vault, this should
    match one of the keys inside the secret. For AWS Secret Manager, it's ignored.
    +optional
    """

    mount_requirement: Optional[MountType] = None
    """
    mount_requirement is optional. Indicates where the secret has to be mounted. If provided, the execution will fail
    if the underlying key management system cannot satisfy that requirement. If not provided, the default location
    will depend on the key management system.
    +optional
    """

    def to_idl(self) -> pb.Secret:
        return pb.Secret(
            group=self.group,
            group_version=self.group_version,
            key=self.key,
            mount_requirement=try_to_idl(self.mount_requirement),
        )


@dataclass
class OAuth2Client:
    """OAuth2Client encapsulates OAuth2 Client Credentials to be used when making calls on behalf of that task."""

    client_id: str
    """
    client_id is the public id for the client to use. The system will not perform any pre-auth validation that the
    secret requested matches the client_id indicated here.
    +required
    """

    client_secret: Secret
    """
    client_secret is a reference to the secret used to authenticate the OAuth2 client.
    +required
    """

    def to_idl(self) -> pb.OAuth2Client:
        return pb.OAuth2Client(
            client_id=self.client_id, client_secret=self.client_secret.to_idl()
        )


@dataclass
class Identity:
    """
    Identity encapsulates the various security identities a task can run as. It's up to the underlying plugin to pick the
    right identity for the execution environment.
    """

    iam_role: str
    """iam_role references the fully qualified name of Identity & Access Management role to impersonate."""

    k8s_service_account: str
    """k8s_service_account references a kubernetes service account to impersonate."""

    oauth2_client: OAuth2Client
    """
    oauth2_client references an oauth2 client. Backend plugins can use this information to impersonate the client when
    making external calls.
    """

    def to_idl(self) -> pb.Identity:
        return pb.Identity(
            iam_role=self.iam_role,
            k8s_service_account=self.k8s_service_account,
            oauth2_client=self.oauth2_client.to_idl(),
        )


@dataclass
class OAuth2TokenRequest:
    """
    OAuth2TokenRequest encapsulates information needed to request an OAuth2 token.
    FLYTE_TOKENS_ENV_PREFIX will be passed to indicate the prefix of the environment variables that will be present if
    tokens are passed through environment variables.
    FLYTE_TOKENS_PATH_PREFIX will be passed to indicate the prefix of the path where secrets will be mounted if tokens
    are passed through file mounts.
    """

    class Type(int, Enum):
        """Type of the token requested."""

        client_credentials = pb.OAuth2TokenRequest.CLIENT_CREDENTIALS
        """CLIENT_CREDENTIALS indicates a 2-legged OAuth token requested using client credentials."""

        def to_idl(self) -> pb.OAuth2TokenRequest.Type:
            return self.value

    name: str
    """
    name indicates a unique id for the token request within this task token requests. It'll be used as a suffix for
    environment variables and as a filename for mounting tokens as files.
    +required
    """

    type: Type
    """
    type indicates the type of the request to make. Defaults to CLIENT_CREDENTIALS.
    +required
    """

    client: OAuth2Client
    """
    client references the client_id/secret to use to request the OAuth2 token.
    +required
    """

    idp_discovery_endpoint: Optional[str] = None
    """
    idp_discovery_endpoint references the discovery endpoint used to retrieve token endpoint and other related
    information.
    +optional
    """

    token_endpoint: Optional[str] = None
    """
    token_endpoint references the token issuance endpoint. If idp_discovery_endpoint is not provided, this parameter is
    mandatory.
    +optional
    """

    def to_idl(self) -> pb.OAuth2TokenRequest:
        return pb.OAuth2TokenRequest(
            name=self.name,
            type=self.type.to_idl(),
            client=self.client.to_idl(),
            idp_discovery_endpoint=self.idp_discovery_endpoint,
            token_endpoint=self.token_endpoint,
        )


@dataclass
class SecurityContext:
    """SecurityContext holds security attributes that apply to tasks."""

    run_as: Identity
    """
    run_as encapsulates the identity a pod should run as. If the task fills in multiple fields here, it'll be up to the
    backend plugin to choose the appropriate identity for the execution engine the task will run on.
    """

    secrets: Iterable[Secret]
    """
    secrets indicate the list of secrets the task needs in order to proceed. Secrets will be mounted/passed to the
    pod as it starts. If the plugin responsible for kicking of the task will not run it on a flyte cluster (e.g. AWS
    Batch), it's the responsibility of the plugin to fetch the secret (which means propeller identity will need access
    to the secret) and to pass it to the remote execution engine.
    """

    tokens: Iterable[OAuth2TokenRequest]
    """
    tokens indicate the list of token requests the task needs in order to proceed. Tokens will be mounted/passed to the
    pod as it starts. If the plugin responsible for kicking of the task will not run it on a flyte cluster (e.g. AWS
    Batch), it's the responsibility of the plugin to fetch the secret (which means propeller identity will need access
    to the secret) and to pass it to the remote execution engine.
    """

    def to_idl(self) -> pb.SecurityContext:
        return pb.SecurityContext(
            run_as=self.run_as.to_idl(),
            secrets=to_idl_many(self.secrets),
            tokens=to_idl_many(self.tokens),
        )
