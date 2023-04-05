from abc import abstractmethod
from dataclasses import dataclass
from typing import List, TypeVar, Union

import graphql.language as l

GQLPrimitive = Union[str, int, float, bool, None]
T = TypeVar("T", bound=GQLPrimitive)

# todo(ayush): completely rewrite this


class Filter:
    @abstractmethod
    def serialize(self):
        raise NotImplementedError


@dataclass(frozen=True)
class StringFilter(Filter):
    value: str
    case_sensitive: bool = True


@dataclass(frozen=True)
class NumberFilter(Filter):
    value: float


@dataclass(frozen=True)
class Equal(StringFilter, NumberFilter):
    """Return results equal to the provided value"""

    def name(self):
        if hasattr(self, "case_sensitive") and not self.case_sensitive:
            return "equalToInsensitive"
        return "equalTo"


@dataclass(frozen=True)
class NotEqual(StringFilter, NumberFilter):
    """Return results not equal to the provided value"""

    def name(self):
        if hasattr(self, "case_sensitive") and not self.case_sensitive:
            return "notEqualToInsensitive"
        return "notEqualTo"


@dataclass(frozen=True)
class In(StringFilter, NumberFilter):
    """Return results included in the provided list of values"""

    def name(self):
        if hasattr(self, "case_sensitive") and not self.case_sensitive:
            return "inInsensitive"
        return "in"


@dataclass(frozen=True)
class NotIn(StringFilter, NumberFilter):
    """Return results not included in the provided list of values"""

    def name(self):
        if hasattr(self, "case_sensitive") and not self.case_sensitive:
            return "notInInsensitive"
        return "notIn"


@dataclass(frozen=True)
class Like(StringFilter):
    """Return results that match the specified pattern.

    Patterns can include `_` which matches any single character and `%` which
    matches any sequence of 0 or more characters.
    """

    def name(self):
        if not self.case_sensitive:
            return "likeInsensitive"
        return "like"


@dataclass(frozen=True)
class NotLike(StringFilter):
    """Return results that do not match the specified pattern.

    Patterns can include `_` which matches any single character and `%` which
    matches any sequence of 0 or more characters.
    """

    def name(self):
        if not self.case_sensitive:
            return "notLikeInsensitive"
        return "notLike"
