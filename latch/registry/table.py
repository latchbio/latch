from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
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
    """Internal cache class to organize information for a `Table`."""

    display_name: Optional[str] = None
    columns: Optional[Dict[str, Column]] = None


@dataclass(frozen=True)
class Table:
    """A python representation of a Registry Table.

    This class mirrors an existing Table in Registry, and provides a limited set
    of functions for introspecting and modifying the underlying Table.

    `Table`s can be instantiated either by calling `Project.list_tables()` or
    directly using their ID. A `Table` object exposes getter methods for its
    display name and its columns, as well as a context-manager based update
    system.

    Attributes:
        id:
            The ID of the underlying Table as a string.
        _cache:
            A private cache for values that need to be queried over the network,
            should not be interacted with directly.
    """

    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str

    def load(self) -> None:
        """Loads all properties at once.

        Performs a GraphQL request and uses the results to populate the
        `display_name` and `columns` properties of the calling Table's cache.
        This is called by `.get_display_name()` and `.get_columns()` when
        `load_if_missing` is set to True (the default).
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
        """Gets the display name of the calling Table, loading it if necessary.

        This function will return the calling Table's display name. If
        `.load()` has not been called yet, and if `load_if_missing` is set to
        True, a call to `.load()` will be made to populate everything.

        Args:
            load_if_missing:
                Keyword-only. Controls whether or not a call to `.load()` will
                be made if the value has not already been queried.
                True by default.

        Returns:
            The display name of the calling Table as a string. Returns None if
            the display name has not been queried yet and `load_if_missing` is
            set to False.
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
        """Gets the columns of the calling Table, loading them if necessary.

        This function will return the calling Table's columns as a dictionary.
        The keys of the dictionary are column names, and its values are `Column`
        objects.

        If `.load()` has not been called yet, and if `load_if_missing` is set to
        True, a call to `.load()` will be made to populate everything.

        Args:
            load_if_missing:
                Keyword-only. Controls whether or not a call to `.load()` will
                be made if the value has not already been queried.
                True by default.

        Returns:
            A dict of the columns of the calling Table. Returns None if the
            columns have not been queried yet and `load_if_missing` is set to
            False.
        """

        if self._cache.columns is None and load_if_missing:
            self.load()

        return self._cache.columns

    def list_records(self, *, page_size: int = 100) -> Iterator[Dict[str, Record]]:
        """Allows for paginated querying of all records in the calling Table.

        This function returns a generator which yields records one page at a
        time. If `.load()` has not been called, this function will invoke it and
        load all of the calling Table's data.

        Pages are dictionaries with keys being Record IDs and values being
        the respective `Record` objects.  Records are returned in ascending
        order by their ID. Each `Record` is returned with values already loaded,
        so calling `Record.load()` is not necessary.

        Args:
            page_size:
                Keyword-only. Will determine the size of each page yielded by
                the returned generator. Must be a positive integer. By default
                set to 100.

        Returns:
            A generator that yields pages of `Records`. Each page is a
            dictionary mapping string IDs to `Record` instances. Each `Record`
            is returned with values already loaded, so calling `.load()` on it
            is not necessary.

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
        upd = TableUpdate(self)
        yield upd
        upd.commit()

        if reload_on_commit:
            self.load()


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
    _record_upserts: List[_TableRecordsUpsertData] = field(
        default_factory=list,
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    table: Table

    def upsert_record_raw_unsafe(self, *, name: str, values: Dict[str, DBValue]):
        self._record_upserts.append(_TableRecordsUpsertData(name, values))

    def upsert_record(self, name: str, **values: RegistryPythonValue):
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

    def commit(self):
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
        self._record_upserts.clear()
