from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from typing import Optional, Protocol, TypeVar

from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp

K = TypeVar("K")
T = TypeVar("T", bound=Message)
R = TypeVar("R", covariant=True)


class HasToIdl(Protocol[R]):
    def to_idl(self) -> R: ...


def try_to_idl(x: Optional[HasToIdl[R]]) -> Optional[R]:
    if x is None:
        return

    return x.to_idl()


def dur_from_td(x: timedelta) -> Duration:
    res = Duration()
    res.FromTimedelta(x)
    return res


def timestamp_from_datetime(x: datetime) -> Timestamp:
    res = Timestamp()
    res.FromDatetime(x)
    return res


def to_idl_many(xs: Iterable[HasToIdl[R]]) -> Iterable[R]:
    return (x.to_idl() for x in xs)


def to_idl_mapping(xs: Mapping[K, HasToIdl[R]]) -> Mapping[K, R]:
    return {k: v.to_idl() for k, v in xs.items()}


def merged_pb(x: T, *mixins: Optional[HasToIdl[T]]) -> T:
    for m in mixins:
        if m is None:
            continue

        x.MergeFrom(m.to_idl())

    return x
