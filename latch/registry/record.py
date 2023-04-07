from __future__ import annotations  # deal with circular type imports

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Literal, Optional, overload

from latch.registry.upstream_types.types import DBType

from .upstream_types.values import EmptyCell

if TYPE_CHECKING:  # deal with circular type imports
    from latch.registry.types import RecordValue


@dataclass
class _Cache:
    name: Optional[str] = None
    types: Optional[Dict[str, DBType]] = None
    values: Optional[Dict[str, RecordValue]] = None


@dataclass(frozen=True)
class Record:
    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str

    def load(self):
        ...

    @overload
    def get_name(self, *, load_if_missing: Literal[True] = True) -> str:
        ...

    @overload
    def get_name(self, *, load_if_missing: Literal[False]) -> Optional[str]:
        ...

    def get_name(self, *, load_if_missing: bool = True) -> Optional[str]:
        if self._cache.name is None and load_if_missing:
            self.load()

        return self._cache.name

    @overload
    def get_type(
        self,
        key: str,
        default: Optional[DBType] = None,
        *,
        load_if_missing: Literal[True] = True,
    ) -> DBType:
        ...

    @overload
    def get_type(
        self,
        key: str,
        default: Optional[DBType] = None,
        *,
        load_if_missing: Literal[False],
    ) -> Optional[DBType]:
        ...

    def get_type(
        self,
        key: str,
        default: Optional[DBType] = None,
        *,
        load_if_missing: bool = True,
    ) -> Optional[DBType]:
        if self._cache.types is None and load_if_missing:
            self.load()

        xs = self._cache.types
        if xs is None:
            return None

        return xs.get(key, default)

    @overload
    def get_value(
        self,
        key: str,
        default: RecordValue = EmptyCell(),
        *,
        load_if_missing: Literal[True] = True,
    ) -> RecordValue:
        ...

    @overload
    def get_value(
        self,
        key: str,
        default: RecordValue = EmptyCell(),
        *,
        load_if_missing: Literal[False],
    ) -> Optional[RecordValue]:
        ...

    def get_value(
        self,
        key: str,
        default: RecordValue = EmptyCell(),
        *,
        load_if_missing: bool = True,
    ) -> Optional[RecordValue]:
        if self._cache.values is None and load_if_missing:
            self.load()

        xs = self._cache.values
        if xs is None:
            return None

        return xs.get(key, default)

    def __repr__(self):
        if self._cache.name is None:
            res = f"Record(id={self.id})"
        else:
            res = f"Record(id={self.id}, name={self._cache.name})"

        if self._cache.values is None:
            return res

        return res + repr({k: v for k, v in self._cache.values.items()})

    def __str__(self):
        if self._cache.name is None:
            res = f"Record(id={self.id})"
        else:
            res = f"Record(id={self.id}, name={self._cache.name})"

        if self._cache.values is None:
            return res

        return res + str({k: v for k, v in self._cache.values.items()})
