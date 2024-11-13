import typing
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import flyteidl.admin.common_pb2 as pb

from ..core.execution import WorkflowExecution
from ..utils import merged_pb, to_idl_many


@dataclass
class EmailNotification:
    """Defines an email notification specification."""

    recipients_email: Iterable[str]
    """
    The list of email addresses recipients for this notification.
    +required
    """

    def to_idl(self) -> pb.EmailNotification:
        return pb.EmailNotification(recipients_email=self.recipients_email)


@dataclass
class PagerDutyNotification:
    """Defines a pager duty notification specification."""

    recipients_email: Iterable[str]
    """
    Currently, PagerDuty notifications leverage email to trigger a notification.
    +required
    """

    def to_idl(self) -> pb.PagerDutyNotification:
        return pb.PagerDutyNotification(recipients_email=self.recipients_email)


@dataclass
class SlackNotification:
    """Defines a slack notification specification."""

    recipients_email: Iterable[str]
    """
    Currently, Slack notifications leverage email to trigger a notification.
    +required
    """

    def to_idl(self) -> pb.SlackNotification:
        return pb.SlackNotification(recipients_email=self.recipients_email)


@dataclass
class Notification:
    """
    Represents a structure for notifications based on execution status.
    The notification content is configured within flyte admin but can be templatized.
    Future iterations could expose configuring notifications with custom content.
    """

    phases: Iterable[WorkflowExecution.Phase]
    """
    A list of phases to which users can associate the notifications to.
    +required
    """

    type: "typing.Union[NotificationTypeEmail, NotificationTypePagerDuty, NotificationTypeSlack]"
    """
    The type of notification to trigger.
    +required
    """

    def to_idl(self) -> pb.Notification:
        return merged_pb(pb.Notification(phases=to_idl_many(self.phases)), self.type)


@dataclass
class NotificationTypeEmail:
    email: EmailNotification

    def to_idl(self) -> pb.Notification:
        return pb.Notification(email=self.email.to_idl())


@dataclass
class NotificationTypePagerDuty:
    pager_duty: PagerDutyNotification

    def to_idl(self) -> pb.Notification:
        return pb.Notification(pager_duty=self.pager_duty.to_idl())


@dataclass
class NotificationTypeSlack:
    slack: SlackNotification

    def to_idl(self) -> pb.Notification:
        return pb.Notification(slack=self.slack.to_idl())


@dataclass
class Labels:
    """
    Label values to be applied to an execution resource.
    In the future a mode (e.g. OVERRIDE, APPEND, etc) can be defined
    to specify how to merge labels defined at registration and execution time.
    """

    values: Mapping[str, str]
    """Map of custom labels to be applied to the execution resource."""

    def to_idl(self) -> pb.Labels:
        return pb.Labels(values=self.values)


@dataclass
class Annotations:
    """
    Annotation values to be applied to an execution resource.
    In the future a mode (e.g. OVERRIDE, APPEND, etc) can be defined
    to specify how to merge annotations defined at registration and execution time.
    """

    values: Mapping[str, str]
    """Map of custom annotations to be applied to the execution resource."""

    def to_idl(self) -> pb.Annotations:
        return pb.Annotations(values=self.values)


@dataclass
class AuthRole:
    """
    Defines permissions associated with executions created by this launch plan spec.
    Use either of these roles when they have permissions required by your workflow execution.
    Deprecated.
    """

    assumable_iam_role: str
    """Defines an optional iam role which will be used for tasks run in executions created with this launch plan."""

    kubernetes_service_account: str
    """Defines an optional kubernetes service account which will be used for tasks run in executions created with this launch plan."""

    def to_idl(self) -> pb.AuthRole:
        return pb.AuthRole(
            assumable_iam_role=self.assumable_iam_role,
            kubernetes_service_account=self.kubernetes_service_account,
        )


@dataclass
class RawOutputDataConfig:
    """
    Encapsulates user settings pertaining to offloaded data (i.e. Blobs, Schema, query data, etc.).
    See https://github.com/flyteorg/flyte/issues/211 for more background information.
    """

    output_location_prefix: str
    """
    Prefix for where offloaded data from user workflows will be written
    e.g. s3://bucket/key or s3://bucket/
    """

    def to_idl(self) -> pb.RawOutputDataConfig:
        return pb.RawOutputDataConfig(
            output_location_prefix=self.output_location_prefix
        )
