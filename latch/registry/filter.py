from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Filter:
    value: str

    def name(self):
        ...

    def __str__(self):
        return f"{{ {self.name()}: {self.value} }}"


@dataclass(frozen=True)
class StringFilter(Filter):
    case_sensitive: bool = True


@dataclass(frozen=True)
class NumberFilter(Filter):
    ...


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

    value: List[str]

    def name(self):
        if hasattr(self, "case_sensitive") and not self.case_sensitive:
            return "inInsensitive"
        return "in"


@dataclass(frozen=True)
class NotIn(StringFilter, NumberFilter):
    """Return results not included in the provided list of values"""

    value: List[str]

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
