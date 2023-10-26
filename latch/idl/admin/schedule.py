import typing
from dataclasses import dataclass
from enum import Enum

import flyteidl.admin.schedule_pb2 as pb

from ..utils import merged_pb


class FixedRateUnit(int, Enum):
    """Represents a frequency at which to run a schedule."""

    minute = pb.MINUTE
    hour = pb.HOUR
    day = pb.DAY

    def to_idl(self) -> pb.FixedRateUnit:
        return self.value


@dataclass
class FixedRate:
    """Option for schedules run at a certain frequency e.g. every 2 minutes."""

    value: int
    unit: FixedRateUnit

    def to_idl(self) -> pb.FixedRate:
        return pb.FixedRate(value=self.value, unit=self.unit.to_idl())


@dataclass
class CronSchedule:
    """Options for schedules to run according to a cron expression."""

    schedule: str
    """
    Standard/default cron implementation as described by https://en.wikipedia.org/wiki/Cron#CRON_expression;
    Also supports nonstandard predefined scheduling definitions
    as described by https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html#CronExpressions
    except @reboot
    """

    offset: str
    """ISO 8601 duration as described by https://en.wikipedia.org/wiki/ISO_8601#Durations"""

    def to_idl(self) -> pb.CronSchedule:
        return pb.CronSchedule(schedule=self.schedule, offset=self.offset)


@dataclass
class Schedule:
    """Defines complete set of information required to trigger an execution on a schedule."""

    ScheduleExpression: "typing.Union[ScheduleExpressionCronExpression, ScheduleExpressionFixedRate, ScheduleExpressionCronSchedule]"

    kickoff_time_input_arg: str
    """Name of the input variable that the kickoff time will be supplied to when the workflow is kicked off."""

    def to_idl(self) -> pb.Schedule:
        return merged_pb(
            pb.Schedule(kickoff_time_input_arg=self.kickoff_time_input_arg),
            self.ScheduleExpression,
        )


@dataclass
class ScheduleExpressionCronExpression:
    """
    Uses AWS syntax: Minutes Hours Day-of-month Month Day-of-week Year
    e.g. for a schedule that runs every 15 minutes: 0/15 * * * ? *

    Deprecated
    """

    cron_expression: str

    def to_idl(self) -> pb.Schedule:
        return pb.Schedule(cron_expression=self.cron_expression)


@dataclass
class ScheduleExpressionFixedRate:
    rate: FixedRate

    def to_idl(self) -> pb.Schedule:
        return pb.Schedule(rate=self.rate.to_idl())


@dataclass
class ScheduleExpressionCronSchedule:
    cron_schedule: CronSchedule

    def to_idl(self) -> pb.Schedule:
        return pb.Schedule(cron_schedule=self.cron_schedule.to_idl())
