from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    TypedDict,
    Union,
    cast,
    overload,
)

import gql
import graphql.language as l
import graphql.language.parser as lp

from latch.gql._execute import execute
from latch.registry.record import NoSuchColumnError, Record
from latch.registry.types import Column, InvalidValue, RecordValue, RegistryPythonValue
from latch.registry.upstream_types.types import DBType
from latch.registry.upstream_types.values import DBValue, EmptyCell
from latch.registry.utils import to_python_literal, to_python_type, to_registry_literal

from ..types.json import JsonValue


class _AllRecordsNode(TypedDict):
    sampleId: str
    sampleName: str
    sampleDataKey: str
    sampleDataValue: DBValue


class _ColumnNode(TypedDict("_ColumnNodeReserved", {"def": DBValue})):
    key: str
    type: DBType


@dataclass
class _Cache:
    display_name: Optional[str] = None
    columns: Optional[Dict[str, Column]] = None


@dataclass(frozen=True)
class Table:
    """Registry table. Contains :class:`records <latch.registry.record.Record>`.

    :meth:`~latch.registry.project.Project.list_tables` is the typical way to get a :class:`Table`.
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
        """(Re-)populate this table instance's cache.

        Future calls to most getters will return immediately without making a network request.

        Always makes a network request.
        """
        data = execute(
            gql.gql("""
                query TableQuery($id: BigInt!) {
                    catalogExperiment(id: $id) {
                        id
                        displayName
                        catalogExperimentColumnDefinitionsByExperimentId {
                            nodes {
                                key
                                type
                                def
                            }
                        }
                    }
                }
                """),
            variables={"id": self.id},
        )["catalogExperiment"]
        # todo(maximsmol): deal with nonexistent tables

        self._cache.display_name = data["displayName"]

        self._cache.columns = {}
        columns: List[_ColumnNode] = data[
            "catalogExperimentColumnDefinitionsByExperimentId"
        ]["nodes"]
        for x in columns:
            py_type = to_python_type(x["type"]["type"])
            if x["type"]["allowEmpty"]:
                py_type = Union[py_type, EmptyCell]

            cur = Column(x["key"], py_type, x["type"])
            self._cache.columns[cur.key] = cur

    # get_display_name

    @overload
    def get_display_name(self, *, load_if_missing: Literal[True] = True) -> str:
        ...

    @overload
    def get_display_name(self, *, load_if_missing: bool) -> Optional[str]:
        ...

    def get_display_name(self, *, load_if_missing: bool = True) -> Optional[str]:
        """Get the display name of this table.

        This is an opaque string that can contain any valid Unicode data.

        Display names are *not unique* and *must never be used as identifiers*.
        Use :attr:`id` instead.

        Args:
            load_if_missing:
                If true, :meth:`load` the display name if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Display name.
        """
        if self._cache.display_name is None and load_if_missing:
            self.load()

        return self._cache.display_name

    # get_columns

    @overload
    def get_columns(
        self, *, load_if_missing: Literal[True] = True
    ) -> Dict[str, Column]:
        ...

    @overload
    def get_columns(self, *, load_if_missing: bool) -> Optional[Dict[str, Column]]:
        ...

    def get_columns(
        self, *, load_if_missing: bool = True
    ) -> Optional[Dict[str, Column]]:
        """Get the columns of this table.

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

    def list_records(self, *, page_size: int = 100) -> Iterator[Dict[str, Record]]:
        """List Registry records contained in this table.

        Args:
            page_size:
                Maximum size of a page of records. The last page may be shorter
                than this value.

        Yields:
            Pages of records. Each page is a mapping between record IDs and
            :class:`records <latch.registry.record.Record>`.
        """

        cols = self.get_columns()

        # todo(maximsmol): because allSamples returns each column as its own
        # row, we can't paginate by samples because we don't know when a sample is finished
        nodes: List[_AllRecordsNode] = execute(
            gql.gql("""
                query TableQuery($id: BigInt!) {
                    catalogExperiment(id: $id) {
                        allSamples {
                            nodes {
                                sampleId
                                sampleName
                                sampleDataKey
                                sampleDataValue
                            }
                        }
                    }
                }
                """),
            {
                "id": self.id,
            },
        )["catalogExperiment"]["allSamples"]["nodes"]
        # todo(maximsmol): deal with nonexistent tables

        record_names: Dict[str, str] = {}
        record_values: Dict[str, Dict[str, RecordValue]] = {}

        for node in nodes:
            record_names[node["sampleId"]] = node["sampleName"]
            vals = record_values.setdefault(node["sampleId"], {})

            col = cols.get(node["sampleDataKey"])
            if col is None:
                continue

            # todo(maximsmol): in the future, allow storing or yielding values that failed to parse
            vals[col.key] = to_python_literal(
                node["sampleDataValue"], col.upstream_type["type"]
            )

        page: Dict[str, Record] = {}
        for id, values in record_values.items():
            for col in cols.values():
                if col.key in values:
                    continue

                if not col.upstream_type["allowEmpty"]:
                    values[col.key] = InvalidValue("")

            cur = Record(id)
            cur._cache.name = record_names[id]
            cur._cache.values = values
            cur._cache.columns = cols
            page[id] = cur

            if len(page) == page_size:
                yield page
                page = {}

        if len(page) > 0:
            yield page

    @contextmanager
    def update(self, *, reload_on_commit: bool = True) -> Iterator["TableUpdate"]:
        """Start an update transaction.

        The transaction will commit when the context manager closes unless an error occurs.

        No changes will occur until the transaction commits.

        The transaction can be cancelled by running :meth:`TableUpdate.clear`
        before closing the context manager.

        Args:
            reload_on_commit:
                If true, :meth:`load` this table after the transaction commits.

        Returns:
            Context manager for the new transaction.
        """

        upd = TableUpdate(self)
        yield upd
        upd.commit()

        if reload_on_commit:
            self.load()

    def __str__(self) -> str:
        name = self.get_display_name(load_if_missing=False)
        if name is not None:
            return f"Table({repr(name)})"

        return repr(self)


def _parse_selection(x: str) -> l.SelectionNode:
    p = lp.Parser(l.Source(x.lstrip()))
    p.expect_token(l.TokenKind.SOF)
    res = p.parse_selection()
    p.expect_token(l.TokenKind.EOF)
    return res


def _name_node(x: str) -> l.NameNode:
    res = l.NameNode()
    res.value = x
    return res


def _obj_field(k: str, x: JsonValue) -> l.ObjectFieldNode:
    res = l.ObjectFieldNode()

    res.name = _name_node(k)
    res.value = _json_value(x)

    return res


def _json_value(x: JsonValue) -> l.ValueNode:
    # note: this does not support enums
    if x is None:
        return l.NullValueNode()

    if isinstance(x, str):
        res = l.StringValueNode()
        res.value = x
        return res

    if isinstance(x, int):
        if isinstance(x, bool):
            res = l.BooleanValueNode()
            res.value = x
            return res

        res = l.IntValueNode()
        res.value = str(x)
        return res

    if isinstance(x, float):
        res = l.FloatValueNode()
        res.value = str(x)
        return res

    if isinstance(x, float):
        res = l.FloatValueNode()
        res.value = str(x)
        return res

    if isinstance(x, list):
        res = l.ListValueNode()
        res.values = tuple(_json_value(el) for el in x)
        return res

    if isinstance(x, dict):
        res = l.ObjectValueNode()
        res.fields = tuple(_obj_field(k, v) for k, v in x.items())
        return res

    raise ValueError(f"cannot Graphql-serialize JSON value of type {type(x)}: {x}")


@dataclass(frozen=True)
class _TableRecordsUpsertData:
    name: str
    values: Dict[str, DBValue]


@dataclass(frozen=True)
class TableUpdate:
    """Ongoing :class:`Table` update transaction.

    Groups requested updates to commit everything together in one network request.

    Transactions are atomic. The entire transaction either commits or fails with an exception.
    """

    # !!!
    # WARNING: if you add more than one update type, the transaction will NOT BE ATOMIC
    # we need to enable a postgraphile plugin to do mutations in transactions
    # !!!

    _record_upserts: List[_TableRecordsUpsertData] = field(
        default_factory=list,
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    table: Table

    def upsert_record_raw_unsafe(
        self, *, name: str, values: Dict[str, DBValue]
    ) -> None:
        """DANGEROUSLY Update or create a record using raw :class:`values <latch.registry.upstream_types.values.DBValue>`.

        Values are not checked against the existing columns.

        A transport error will be thrown if non-existent columns are updated.

        The update will succeed if values do not match column types and future
        reads will produce :class:`invalid values <latch.registry.types.InvalidValue>`.

        Unsafe:
            The value dictionary is not validated in any way.
            It is possible to create completely invalid record values that
            are not a valid Registry value of any type. Future reads will
            fail catastrophically when trying to parse these values.

        Args:
            name: Target record name.
            values: Column values that to set.
        """
        self._record_upserts.append(_TableRecordsUpsertData(name, values))

    def upsert_record(self, name: str, **values: RegistryPythonValue) -> None:
        """Update or create a record using.

        A transport error will be thrown if non-existent columns are updated.

        It is possible that the column definitions changed since the table was last
        loaded and the update will be issued with values that do not match current column types.
        This will succeed with no error and future reads will produce :class:`invalid values <latch.registry.types.InvalidValue>`.

        Args:
            name: Target record name.
            values: Column values to set.
        """
        cols = self.table.get_columns()

        db_vals: Dict[str, DBValue] = {}
        for k, v in values.items():
            col = cols.get(k)
            if col is None:
                raise NoSuchColumnError(k)

            db_vals[k] = to_registry_literal(v, col.upstream_type["type"])

        self._record_upserts.append(_TableRecordsUpsertData(name, db_vals))

    def _add_record_upserts_selection(self, updates: List[l.SelectionNode]) -> None:
        if len(self._record_upserts) == 0:
            return

        names: JsonValue = [x.name for x in self._record_upserts]
        values: JsonValue = [
            cast(Dict[str, JsonValue], x.values) for x in self._record_upserts
        ]

        res = _parse_selection("""
            catalogMultiUpsertSamples(input: {}) {
                clientMutationId
            }
        """)
        assert isinstance(res, l.FieldNode)

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value(
            {
                "argExperimentId": self.table.id,
                "argNames": names,
                "argData": values,
            }
        )

        res.alias = _name_node(f"upd{len(updates)}")
        res.arguments = tuple([args])

        updates.append(res)

    def commit(self) -> None:
        """Commit this table update transaction.

        May be called multiple times.

        All pending updates are committed with one network request.

        Atomic. The entire transaction either commits or fails with an exception.
        """
        updates: List[l.SelectionNode] = []

        self._add_record_upserts_selection(updates)

        if len(updates) == 0:
            return

        sel_set = l.SelectionSetNode()
        sel_set.selections = tuple(updates)

        doc = l.parse("""
            mutation TableUpdate {
                placeholder
            }
        """)

        assert len(doc.definitions) == 1
        mut = doc.definitions[0]

        assert isinstance(mut, l.OperationDefinitionNode)
        mut.selection_set = sel_set

        execute(doc)

        self.clear()

    def clear(self):
        """Remove pending updates.

        May be called to cancel any pending updates that have not been committed.
        """
        self._record_upserts.clear()
