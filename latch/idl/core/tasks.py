import typing
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional

import flyteidl.core.tasks_pb2 as pb
import google.protobuf.struct_pb2 as pb_struct

from ..utils import dur_from_td, merged_pb, to_idl_many
from .identifier import Identifier
from .interface import TypedInterface
from .literals import KeyValuePair, RetryStrategy
from .security import SecurityContext


@dataclass
class Resources:
    """
    A customizable interface to convey resources requested for a container. This can be interpreted differently for different
    container engines.
    """

    class ResourceName(int, Enum):
        """Known resource names."""

        unknown = pb.Resources.UNKNOWN
        cpu = pb.Resources.CPU
        gpu = pb.Resources.GPU
        memory = pb.Resources.MEMORY
        storage = pb.Resources.STORAGE
        ephemeral_storage = pb.Resources.EPHEMERAL_STORAGE
        """For Kubernetes-based deployments, pods use ephemeral local storage for scratch space, caching, and for logs."""

        def to_idl(self) -> pb.Resources.ResourceName:
            return self.value

    @dataclass
    class ResourceEntry:
        """Encapsulates a resource name and value."""

        name: "Resources.ResourceName"
        """Resource name."""

        value: str
        """
        Value must be a valid k8s quantity. See
        https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go#L30-L80
        """

        def to_idl(self) -> pb.Resources.ResourceEntry:
            return pb.Resources.ResourceEntry(name=self.name.to_idl(), value=self.value)

    requests: Iterable[ResourceEntry]
    """The desired set of resources requested. ResourceNames must be unique within the list."""

    limits: Iterable[ResourceEntry]
    """
    Defines a set of bounds (e.g. min/max) within which the task can reliably run. ResourceNames must be unique
    within the list.
    """

    def to_idl(self) -> pb.Resources:
        return pb.Resources(
            requests=to_idl_many(self.requests),
            limits=to_idl_many(self.limits),
        )


@dataclass
class RuntimeMetadatta:
    """Runtime information. This is loosely defined to allow for extensibility."""

    class RuntimeType(int, Enum):
        other = pb.RuntimeMetadata.OTHER
        flyte_sdk = pb.RuntimeMetadata.FLYTE_SDK

        def to_idl(self) -> pb.RuntimeMetadata.RuntimeType:
            return self.value

    type: RuntimeType
    """Type of runtime."""

    version: str
    """
    Version of the runtime. All versions should be backward compatible. However, certain cases call for version
    checks to ensure tighter validation or setting expectations.
    """

    flavor: str
    """+optional It can be used to provide extra information about the runtime (e.g. python, golang... etc.)."""

    def to_idl(self) -> pb.RuntimeMetadata:
        return pb.RuntimeMetadata(
            type=self.type.to_idl(), version=self.version, flavor=self.flavor
        )


@dataclass
class TaskMetadata:
    """Task Metadata"""

    discoverable: bool
    """Indicates whether the system should attempt to lookup this task's output to avoid duplication of work."""

    runtime: RuntimeMetadatta
    """Runtime information about the task."""

    timeout: timedelta
    """The overall timeout of a task including user-triggered retries."""

    retries: RetryStrategy
    """Number of retries per task."""

    discovery_version: str
    """Indicates a logical version to apply to this task for the purpose of discovery."""

    deprecated_error_message: str
    """
    If set, this indicates that this task is deprecated. This will enable owners of tasks to notify consumers
    of the ending of support for a given task.
    """

    cache_serializable: bool
    """Indicates whether the system should attempt to execute discoverable instances in serial to avoid duplicate work"""

    interruptible: Optional[bool] = None
    """
    Identify whether task is interruptible

    For interruptible we will populate it at the node level but require it be part of TaskMetadata
    for a user to set the value.
    We are using oneof instead of bool because otherwise we would be unable to distinguish between value being
    set by the user or defaulting to false.
    The logic of handling precedence will be done as part of flytepropeller.
    """

    def to_idl(self) -> pb.TaskMetadata:
        res = pb.TaskMetadata(
            discoverable=self.discoverable,
            runtime=self.runtime.to_idl(),
            timeout=dur_from_td(self.timeout),
            retries=self.retries.to_idl(),
            discovery_version=self.discovery_version,
            deprecated_error_message=self.deprecated_error_message,
            cache_serializable=self.cache_serializable,
        )

        if self.interruptible is not None:
            res.interruptible = self.interruptible

        return res


@dataclass
class TaskTemplate:
    """
    A Task structure that uniquely identifies a task in the system
    Tasks are registered as a first step in the system.
    """

    id: Identifier
    """Auto generated taskId by the system. Task Id uniquely identifies this task globally."""

    type: str
    """
    A predefined yet extensible Task type identifier. This can be used to customize any of the components. If no
    extensions are provided in the system, Flyte will resolve the this task to its TaskCategory and default the
    implementation registered for the TaskCategory.
    """

    metadata: TaskMetadata
    """Extra metadata about the task."""

    interface: TypedInterface
    """
    A strongly typed interface for the task. This enables others to use this task within a workflow and guarantees
    compile-time validation of the workflow to avoid costly runtime failures.
    """

    custom: pb_struct.Struct
    """Custom data about the task. This is extensible to allow various plugins in the system."""

    target: "typing.Union[TaskTemplateTargetContainer, TaskTemplateTargetK8sPod, TaskTemplateTargetSql]"
    """
    Known target types that the system will guarantee plugins for. Custom SDK plugins are allowed to set these if needed.
    If no corresponding execution-layer plugins are found, the system will default to handling these using built-in
    handlers.
    """

    task_type_version: int
    """This can be used to customize task handling at execution time for the same task type."""

    security_context: SecurityContext
    """security_context encapsulates security attributes requested to run this task."""

    config: Mapping[str, str]
    """
    Metadata about the custom defined for this task. This is extensible to allow various plugins in the system
    to use as required.
    reserve the field numbers 1 through 15 for very frequently occurring message elements
    """

    def to_idl(self) -> pb.TaskTemplate:
        return merged_pb(
            pb.TaskTemplate(
                id=self.id.to_idl(),
                type=self.type,
                metadata=self.metadata.to_idl(),
                interface=self.interface.to_idl(),
                custom=self.custom,
                task_type_version=self.task_type_version,
                security_context=self.security_context.to_idl(),
                config=self.config,
            ),
            self.target,
        )


@dataclass
class TaskTemplateTargetContainer:
    container: "Container"

    def to_idl(self) -> pb.TaskTemplate:
        return pb.TaskTemplate(container=self.container.to_idl())


@dataclass
class TaskTemplateTargetK8sPod:
    k8s_pod: "K8sPod"

    def to_idl(self) -> pb.TaskTemplate:
        return pb.TaskTemplate(k8s_pod=self.k8s_pod.to_idl())


@dataclass
class TaskTemplateTargetSql:
    sql: "Sql"

    def to_idl(self) -> pb.TaskTemplate:
        return pb.TaskTemplate(sql=self.sql.to_idl())


@dataclass
class ContainerPort:
    """Defines port properties for a container."""

    container_port: int
    """
    Number of port to expose on the pod's IP address.
    This must be a valid port number, 0 < x < 65536.
    """

    def to_idl(self) -> pb.ContainerPort:
        return pb.ContainerPort(container_port=self.container_port)


@dataclass
class Container:
    image: str
    """Container image url. Eg: docker/redis:latest"""

    command: Iterable[str]
    """Command to be executed, if not provided, the default entrypoint in the container image will be used."""

    args: Iterable[str]
    """
    These will default to Flyte given paths. If provided, the system will not append known paths. If the task still
    needs flyte's inputs and outputs path, add $(FLYTE_INPUT_FILE), $(FLYTE_OUTPUT_FILE) wherever makes sense and the
    system will populate these before executing the container.
    """

    resources: Resources
    """Container resources requirement as specified by the container engine."""

    env: Iterable[KeyValuePair]
    """Environment variables will be set as the container is starting up."""

    config: Iterable[KeyValuePair]
    """
    Allows extra configs to be available for the container.
    TODO: elaborate on how configs will become available.
    Deprecated, please use TaskTemplate.config instead.
    """

    ports: Iterable[ContainerPort]
    """
    Ports to open in the container. This feature is not supported by all execution engines. (e.g. supported on K8s but
    not supported on AWS Batch)
    Only K8s
    """

    data_config: "DataLoadingConfig"
    """
    BETA: Optional configuration for DataLoading. If not specified, then default values are used.
    This makes it possible to to run a completely portable container, that uses inputs and outputs
    only from the local file-system and without having any reference to flyteidl. This is supported only on K8s at the moment.
    If data loading is enabled, then data will be mounted in accompanying directories specified in the DataLoadingConfig. If the directories
    are not specified, inputs will be mounted onto and outputs will be uploaded from a pre-determined file-system path. Refer to the documentation
    to understand the default paths.
    Only K8s
    """

    class Architecture(int, Enum):
        unknown = pb.Container.UNKNOWN
        amd64 = pb.Container.AMD64
        arm64 = pb.Container.ARM64
        arm_v6 = pb.Container.ARM_V6
        arm_v7 = pb.Container.ARM_V7

        def to_idl(self) -> pb.Container.Architecture:
            return self.value

    architecture: Architecture
    """Architecture-type the container image supports."""

    def to_idl(self) -> pb.Container:
        return pb.Container(
            image=self.image,
            command=self.command,
            args=self.args,
            resources=self.resources.to_idl(),
            env=to_idl_many(self.env),
            config=to_idl_many(self.config),
            ports=to_idl_many(self.ports),
            data_config=self.data_config.to_idl(),
            architecture=self.architecture.to_idl(),
        )


@dataclass
class IOStrategy:
    """Strategy to use when dealing with Blob, Schema, or multipart blob data (large datasets)"""

    class DownloadMode(int, Enum):
        """Mode to use for downloading"""

        download_eager = pb.IOStrategy.DOWNLOAD_EAGER
        """All data will be downloaded before the main container is executed"""
        download_stream = pb.IOStrategy.DOWNLOAD_STREAM
        """Data will be downloaded as a stream and an End-Of-Stream marker will be written to indicate all data has been downloaded. Refer to protocol for details"""
        do_not_download = pb.IOStrategy.DO_NOT_DOWNLOAD
        """Large objects (offloaded) will not be downloaded"""

        def to_idl(self) -> pb.IOStrategy.DownloadMode:
            return self.value

    class UploadMode(int, Enum):
        """Mode to use for uploading"""

        uplaod_on_exit = pb.IOStrategy.UPLOAD_ON_EXIT
        """All data will be uploaded after the main container exits"""
        upload_eager = pb.IOStrategy.UPLOAD_EAGER
        """Data will be uploaded as it appears. Refer to protocol specification for details"""
        do_not_upload = pb.IOStrategy.DO_NOT_UPLOAD
        """Data will not be uploaded, only references will be written"""

        def to_idl(self) -> pb.IOStrategy.UploadMode:
            return self.value

    download_mode: DownloadMode
    """Mode to use to manage downloads"""
    upload_mode: UploadMode
    """Mode to use to manage uploads"""

    def to_idl(self) -> pb.IOStrategy:
        return pb.IOStrategy(
            download_mode=self.download_mode.to_idl(),
            upload_mode=self.upload_mode.to_idl(),
        )


@dataclass
class DataLoadingConfig:
    """
    This configuration allows executing raw containers in Flyte using the Flyte CoPilot system.
    Flyte CoPilot, eliminates the needs of flytekit or sdk inside the container. Any inputs required by the users container are side-loaded in the input_path
    Any outputs generated by the user container - within output_path are automatically uploaded.
    """

    class LiteralMapFormat(int, Enum):
        """
        LiteralMapFormat decides the encoding format in which the input metadata should be made available to the containers.
        If the user has access to the protocol buffer definitions, it is recommended to use the PROTO format.
        JSON and YAML do not need any protobuf definitions to read it
        All remote references in core.LiteralMap are replaced with local filesystem references (the data is downloaded to local filesystem)
        """

        json = pb.DataLoadingConfig.JSON
        """JSON for the metadata (which contains inlined primitive values). The representation is inline with the standard json specification as specified - https://www.json.org/json-en.html"""
        yaml = pb.DataLoadingConfig.YAML
        """YAML for the metadata (which contains inlined primitive values)"""
        proto = pb.DataLoadingConfig.PROTO
        """Proto is a serialized binary of `core.LiteralMap` defined in flyteidl/core"""

        def to_idl(self) -> pb.DataLoadingConfig.LiteralMapFormat:
            return self.value

    enabled: bool
    """Flag enables DataLoading Config. If this is not set, data loading will not be used!"""

    input_path: str
    """
    File system path (start at root). This folder will contain all the inputs exploded to a separate file.
    Example, if the input interface needs (x: int, y: blob, z: multipart_blob) and the input path is "/var/flyte/inputs", then the file system will look like
    /var/flyte/inputs/inputs.<metadata format dependent -> .pb .json .yaml> -> Format as defined previously. The Blob and Multipart blob will reference local filesystem instead of remote locations
    /var/flyte/inputs/x -> X is a file that contains the value of x (integer) in string format
    /var/flyte/inputs/y -> Y is a file in Binary format
    /var/flyte/inputs/z/... -> Note Z itself is a directory
    More information about the protocol - refer to docs #TODO reference docs here
    """

    output_path: str
    """File system path (start at root). This folder should contain all the outputs for the task as individual files and/or an error text file"""

    format: LiteralMapFormat
    """
    In the inputs folder, there will be an additional summary/metadata file that contains references to all files or inlined primitive values.
    This format decides the actual encoding for the data. Refer to the encoding to understand the specifics of the contents and the encoding
    """

    io_strategy: IOStrategy

    def to_idl(self) -> pb.DataLoadingConfig:
        return pb.DataLoadingConfig(
            enabled=self.enabled,
            input_path=self.input_path,
            output_path=self.output_path,
            format=self.format.to_idl(),
            io_strategy=self.io_strategy.to_idl(),
        )


@dataclass
class K8sPod:
    """Defines a pod spec and additional pod metadata that is created when a task is executed."""

    metadata: "K8sObjectMetadata"
    """Contains additional metadata for building a kubernetes pod."""

    pod_spec: pb_struct.Struct
    """
    Defines the primary pod spec created when a task is executed.
    This should be a JSON-marshalled pod spec, which can be defined in
    - go, using: https://github.com/kubernetes/api/blob/release-1.21/core/v1/types.go#L2936
    - python: using https://github.com/kubernetes-client/python/blob/release-19.0/kubernetes/client/models/v1_pod_spec.py
    """

    def to_idl(self) -> pb.K8sPod:
        return pb.K8sPod(metadata=self.metadata.to_idl(), pod_spec=self.pod_spec)


@dataclass
class K8sObjectMetadata:
    """Metadata for building a kubernetes object when a task is executed."""

    labels: Mapping[str, str]
    """Optional labels to add to the pod definition."""

    annotations: Mapping[str, str]
    """Optional annotations to add to the pod definition."""

    def to_idl(self) -> pb.K8sObjectMetadata:
        return pb.K8sObjectMetadata(labels=self.labels, annotations=self.annotations)


@dataclass
class Sql:
    """Sql represents a generic sql workload with a statement and dialect."""

    statement: str
    """
    The actual query to run, the query can have templated parameters.
    We use Flyte's Golang templating format for Query templating.
    Refer to the templating documentation.
    https://docs.flyte.org/projects/cookbook/en/latest/auto/integrations/external_services/hive/hive.html#sphx-glr-auto-integrations-external-services-hive-hive-py
    For example,
    insert overwrite directory '{{ .rawOutputDataPrefix }}' stored as parquet
    select *
    from my_table
    where ds = '{{ .Inputs.ds }}'
    """

    class Dialect(int, Enum):
        undefined = pb.Sql.UNDEFINED
        ansi = pb.Sql.ANSI
        hive = pb.Sql.HIVE
        other = pb.Sql.OTHER

        def to_idl(self) -> pb.Sql.Dialect:
            return self.value

    dialect: Dialect
    """
    The dialect of the SQL statement. This is used to validate and parse SQL statements at compilation time to avoid
    expensive runtime operations. If set to an unsupported dialect, no validation will be done on the statement.
    We support the following dialect: ansi, hive.
    """

    def to_idl(self) -> pb.Sql:
        return pb.Sql(statement=self.statement, dialect=self.dialect.to_idl())
