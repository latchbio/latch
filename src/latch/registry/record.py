from __future__ import annotations  # avoid circular type imports

import datetime
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypedDict,
    overload,
)

import dateutil.parser as dp
import gql

from latch.registry.upstream_types.types import DBType
from latch.registry.upstream_types.values import DBValue
from latch.utils import NotFoundError, current_workspace
from latch_sdk_gql.execute import execute

if TYPE_CHECKING:  # avoid circular type imports
    from latch.registry.types import Column, RecordValue


class RecordNotFoundError(NotFoundError): ...


class NoSuchColumnError(KeyError):
    """Unknown column referenced by Registry method.

    Reloading the containing table might help.

    Attributes:
        key: The unknown column key.
    """

    def __init__(self, key: str):
        super().__init__(f"no such column: {key}")

        self.key = key


class _ColumnDefinition(TypedDict("_ColumnDefinitionReserved", {"def": DBValue})):
    key: str
    type: DBType


class _ColumnDefinitionConnection(TypedDict):
    nodes: List[_ColumnDefinition]


class _CatalogExperiment(TypedDict):
    id: str
    catalogExperimentColumnDefinitionsByExperimentId: _ColumnDefinitionConnection


class _ColumnDataNode(TypedDict):
    key: str
    data: DBValue


class _ColumnDataConnection(TypedDict):
    nodes: List[_ColumnDataNode]


class CatalogEvent(TypedDict):
    time: str


class _CatalogEventsConnection(TypedDict):
    nodes: list[CatalogEvent]


class _CatalogSample(TypedDict):
    id: str
    name: str
    creationTime: str
    catalogEventsBySampleId: _CatalogEventsConnection
    experiment: _CatalogExperiment
    catalogSampleColumnDataBySampleId: _ColumnDataConnection


@dataclass
class _Cache:
    """Internal cache class to organize information for a `Record`."""

    table_id: Optional[str] = None
    name: Optional[str] = None
    creation_time: Optional[datetime.datetime] = None
    last_updated: Optional[datetime.datetime] = None
    columns: Optional[Dict[str, Column]] = None
    values: Optional[Dict[str, RecordValue]] = None


@dataclass(frozen=True)
class Record:
    """Registry record.

    :meth:`~latch.registry.table.Table.list_records` is the typical way to get a :class:`Record`.
    """

    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str
    """Unique identifier."""

    def load(self) -> None:
        """(Re-)populate this record instance's cache.

        Future calls to most getters will return immediately without making a network request.

        Always makes a network request.
        """
        # avoid circular type imports
        from latch.registry.types import Column, InvalidValue
        from latch.registry.utils import to_python_literal, to_python_type

        data: _CatalogSample = execute(
            gql.gql("""
            query RecordQuery($id: BigInt!) {
                catalogSample(id: $id) {
                    id
                    name
                    creationTime
                    catalogEventsBySampleId(orderBy: TIME_DESC, first: 1) {
                        nodes {
                            time
                        }
                    }
                    catalogSampleColumnDataBySampleId {
                        nodes {
                            key
                            data
                        }
                    }
                    experiment {
                        id
                        catalogExperimentColumnDefinitionsByExperimentId {
                            nodes {
                                type
                                key
                                def
                            }
                        }
                    }
                }
            }
            """),
            {"id": self.id},
        )["catalogSample"]

        if data is None:
            raise RecordNotFoundError(
                f"record does not exist or you lack permissions: id={self.id}"
            )

        self._cache.table_id = data["experiment"]["id"]
        self._cache.name = data["name"]
        self._cache.creation_time = dp.isoparse(data["creationTime"])

        events = data["catalogEventsBySampleId"]["nodes"]
        self._cache.last_updated = dp.isoparse(data["creationTime"])
        if len(events) > 0:
            self._cache.last_updated = dp.isoparse(events[0]["time"])

        typeNodes = data["experiment"][
            "catalogExperimentColumnDefinitionsByExperimentId"
        ]["nodes"]
        # fixme(maximsmol): deal with defaults
        self._cache.columns = {
            n["key"]: Column(n["key"], to_python_type(n["type"]["type"]), n["type"])
            for n in typeNodes
        }

        valNodes = data["catalogSampleColumnDataBySampleId"]["nodes"]
        colVals = {n["key"]: n["data"] for n in valNodes}

        vals: Dict[str, RecordValue] = {}
        for k, v in colVals.items():
            col = self._cache.columns.get(k)
            if col is None:
                raise NoSuchColumnError(k)

            # todo(maximsmol): allow creating records with mismatching types
            vals[k] = to_python_literal(v, col.upstream_type["type"])

        for k, c in self._cache.columns.items():
            if k in vals:
                continue

            if not c.upstream_type["allowEmpty"]:
                vals[k] = InvalidValue("")

            # prevent keyerrors when accessing columns that don't have a value
            vals[k] = None

        self._cache.values = vals

    # get_table_id

    @overload
    def get_table_id(self, *, load_if_missing: Literal[True] = True) -> str: ...

    @overload
    def get_table_id(self, *, load_if_missing: bool) -> Optional[str]: ...

    def get_table_id(self, *, load_if_missing: bool = True) -> Optional[str]:
        """Get the ID of the table that contains this record.

        Args:
            load_if_missing:
                If true, :meth:`load` the table ID if not in cache.
                If false, return `None` if not in cache.

        Returns:
            ID of the :class:`Table` containing this record.
        """
        if self._cache.table_id is None:
            if not load_if_missing:
                return None

            self.load()

        return self._cache.table_id

    @overload
    def get_creation_time(
        self, *, load_if_missing: Literal[True] = True
    ) -> datetime.datetime: ...

    @overload
    def get_creation_time(
        self, *, load_if_missing: bool
    ) -> Optional[datetime.datetime]: ...

    def get_creation_time(
        self, *, load_if_missing: bool = True
    ) -> Optional[datetime.datetime]:
        """Get the creation time of this record.

        Args:
            load_if_missing:
                If true, :meth:`load` the creation time if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Creation time of this record.
        """
        if self._cache.creation_time is None and load_if_missing:
            self.load()

        return self._cache.creation_time

    @overload
    def get_last_updated(
        self, *, load_if_missing: Literal[True] = True
    ) -> datetime.datetime: ...

    @overload
    def get_last_updated(
        self, *, load_if_missing: bool
    ) -> Optional[datetime.datetime]: ...

    def get_last_updated(
        self, *, load_if_missing: bool = True
    ) -> Optional[datetime.datetime]:
        """Get the time of the last update of this record.

        Args:
            load_if_missing:
                If true, :meth:`load` the time of the last update if not in cache.
                If false, return `None` if not in cache.

        Returns:
            The last time this record was modified.
        """
        if self._cache.last_updated is None and load_if_missing:
            self.load()

        return self._cache.last_updated

    # get_name

    @overload
    def get_name(self, *, load_if_missing: Literal[True] = True) -> str: ...

    @overload
    def get_name(self, *, load_if_missing: bool) -> Optional[str]: ...

    def get_name(self, *, load_if_missing: bool = True) -> Optional[str]:
        """Get the name of this record.

        Names are unique within a table. Names are not globally unique.
        Use :attr:`id` if a globally unique identifier is required.

        Args:
            load_if_missing:
                If true, :meth:`load` the name if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Name of this record.
        """
        if self._cache.name is None and load_if_missing:
            self.load()

        return self._cache.name

    # get_columns

    @overload
    def get_columns(
        self, *, load_if_missing: Literal[True] = True
    ) -> Dict[str, Column]: ...

    @overload
    def get_columns(self, *, load_if_missing: bool) -> Optional[Dict[str, Column]]: ...

    def get_columns(
        self, *, load_if_missing: bool = True
    ) -> Optional[Dict[str, Column]]:
        """Get the columns of this record's table.

        Args:
            load_if_missing:
                If true, :meth:`load` the column list if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Mapping between column keys and :class:`columns <latch.registry.types.Column>`.
        """
        if self._cache.columns is None and load_if_missing:
            self.load()

        return self._cache.columns

    # get_values

    @overload
    def get_values(
        self, *, load_if_missing: Literal[True] = True
    ) -> Dict[str, RecordValue]: ...

    @overload
    def get_values(
        self, *, load_if_missing: bool
    ) -> Optional[Dict[str, RecordValue]]: ...

    def get_values(
        self, *, load_if_missing: bool = True
    ) -> Optional[Dict[str, RecordValue]]:
        """Get this record's values.

        The resulting dictionary is shared between all calls to :meth:`get_values`.
        Make deep copies if independent mutation is desired.

        Args:
            load_if_missing:
                If true, :meth:`load` the values if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Mapping between column keys and the corresponding value.
        """
        if self._cache.values is None and load_if_missing:
            self.load()

        return self._cache.values

    def _repr_parts(self) -> Tuple[str, Optional[Dict[str, RecordValue]]]:
        name = self.get_name(load_if_missing=False)
        if name is None:
            base = f"Record(id={self.id})"
        else:
            base = f"Record(id={self.id}, name={name})"

        values = self.get_values(load_if_missing=False)
        return base, values

    def __repr__(self):
        base, vals = self._repr_parts()

        if vals is None:
            return base

        return base + repr(vals)

    def __str__(self):
        base, vals = self._repr_parts()

        if vals is None:
            return base

        return base + str(vals)
