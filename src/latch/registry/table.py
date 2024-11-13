import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import (
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypedDict,
    Union,
    cast,
    get_args,
    get_origin,
    overload,
)

import gql
import gql.transport.exceptions
import graphql.language as l
import graphql.language.parser as lp
from latch_sdk_gql.execute import execute
from latch_sdk_gql.utils import (
    _GqlJsonValue,
    _json_value,
    _name_node,
    _parse_selection,
    _var_def_node,
    _var_node,
)
from typing_extensions import TypeAlias

from latch.registry.record import NoSuchColumnError, Record
from latch.registry.types import (
    Column,
    InvalidValue,
    LinkedRecordType,
    RecordValue,
    RegistryEnumDefinition,
    RegistryPythonType,
    RegistryPythonValue,
)
from latch.registry.upstream_types.types import DBType, RegistryType
from latch.registry.upstream_types.values import DBValue, EmptyCell, UnresolvedBlobValue
from latch.registry.utils import (
    RegistryTransformerException,
    _get_unresolved_blobs_in_update,
    to_python_literal,
    to_python_type,
    to_registry_literal,
)
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.utils import NotFoundError, current_workspace
from latch_cli.utils import human_readable_time

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
    project_id: Optional[str] = None


class TableNotFoundError(NotFoundError): ...


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
                        projectId
                    }
                }
                """),
            variables={"id": self.id},
        )["catalogExperiment"]

        if data is None:
            raise TableNotFoundError(
                f"table does not exist or you lack permissions: id={self.id}"
            )

        self._cache.project_id = data["projectId"]
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

    # get_project_id

    @overload
    def get_project_id(self, *, load_if_missing: Literal[True] = True) -> str: ...

    @overload
    def get_project_id(self, *, load_if_missing: bool) -> Optional[str]: ...

    def get_project_id(self, *, load_if_missing: bool = True) -> Optional[str]:
        """Get the ID of the project that contains this table.

        Args:
            load_if_missing:
                If true, :meth:`load` the project ID if not in cache.
                If false, return `None` if not in cache.

        Returns:
            ID of the :class:`Project` containing this table.
        """
        if self._cache.project_id is None:
            if not load_if_missing:
                return None

            self.load()

        return self._cache.project_id

    # get_display_name

    @overload
    def get_display_name(self, *, load_if_missing: Literal[True] = True) -> str: ...

    @overload
    def get_display_name(self, *, load_if_missing: bool) -> Optional[str]: ...

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
    ) -> Dict[str, Column]: ...

    @overload
    def get_columns(self, *, load_if_missing: bool) -> Optional[Dict[str, Column]]: ...

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
        data = execute(
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
            {"id": self.id},
        )["catalogExperiment"]

        if data is None:
            raise TableNotFoundError(
                f"table does not exist or you lack permissions: id={self.id}"
            )

        nodes: List[_AllRecordsNode] = data["allSamples"]["nodes"]

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

    def get_dataframe(self):
        """Get a pandas DataFrame of all records in this table.

        Returns:
            DataFrame representing all records in this table.
        """

        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas needs to be installed to use get_dataframe. Install it with"
                " `pip install pandas` or `pip install latch[pandas]`."
            )

        records = []
        for page in self.list_records():
            for record in page.values():
                full_record = record.get_values()
                if full_record is not None:
                    full_record["Name"] = record.get_name()
                    records.append(full_record)

        if len(records) == 0:
            cols = self.get_columns()
            if cols is None:
                return pd.DataFrame()

            return pd.DataFrame(columns=list(cols.keys()))

        return pd.DataFrame(records)

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

    def __repr__(self):
        display_name = self.get_display_name(load_if_missing=False)
        if display_name is not None:
            return f"Table(id={self.id}, display_name={display_name})"

        return f"Table(id={self.id})"

    def __str__(self):
        return repr(self)


@dataclass(frozen=True)
class _TableRecordsUpsertData:
    name: str
    values: Dict[str, DBValue]


@dataclass(frozen=True)
class _TableRecordsDeleteData:
    name: str


@dataclass(frozen=True)
class _TableColumnUpsertData:
    key: str
    type: DBType


_TableRecordsMutationData: TypeAlias = Union[
    _TableRecordsUpsertData,
    _TableRecordsDeleteData,
    _TableColumnUpsertData,
]


class InvalidColumnTypeError(ValueError):
    """Failure to use an invalid column type.

    Attributes:
        key: Identifier of the invalid column.
        invalid_type: Requested column type.
    """

    def __init__(
        self, key: str, invalid_type: Union[Type[object], RegistryPythonType], msg: str
    ):
        super().__init__(
            f"invalid column type for {repr(key)}. {msg}: {repr(invalid_type)}"
        )

        self.key = key
        self.invalid_type = invalid_type


@dataclass(frozen=True)
class TableUpdate:
    """Ongoing :class:`Table` update transaction.

    Groups requested updates to commit everything together in one network request.

    Transactions are atomic. The entire transaction either commits or fails with an exception.
    """

    _record_mutations: List[_TableRecordsMutationData] = field(
        default_factory=list,
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    table: Table

    # upsert record

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
        self._record_mutations.append(_TableRecordsUpsertData(name, values))

    def upsert_record(self, name: str, **values: RegistryPythonValue) -> None:
        """Update or create a record.

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

            db_vals[k] = to_registry_literal(
                v, col.upstream_type["type"], resolve_paths=False
            )

        self._record_mutations.append(_TableRecordsUpsertData(name, db_vals))

    def _add_record_upserts_selection(
        self,
        upserts: List[_TableRecordsUpsertData],
        mutations: List[l.SelectionNode],
        vars: Dict[str, Tuple[l.TypeNode, JsonValue]],
    ) -> None:
        if len(upserts) == 0:
            return

        names: _GqlJsonValue = [x.name for x in upserts]
        values: JsonValue = [cast(Dict[str, JsonValue], x.values) for x in upserts]

        res = _parse_selection("""
            catalogMultiUpsertSamples(input: {}) {
                clientMutationId
            }
        """)
        assert isinstance(res, l.FieldNode)

        argDataVar = f"upd{len(mutations)}ArgData"

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value({
            "argExperimentId": self.table.id,
            "argNames": names,
            "argData": _var_node(argDataVar),
        })

        res.alias = _name_node(f"upd{len(mutations)}")
        res.arguments = tuple([args])

        mutations.append(res)
        vars[argDataVar] = (l.parse_type("[JSON]"), values)

    # delete record

    def delete_record(self, name: str) -> None:
        """Delete a record.

        Args:
            name: Target record name.
        """
        self._record_mutations.append(_TableRecordsDeleteData(name))

    def _add_record_deletes_selection(
        self, deletes: List[_TableRecordsDeleteData], mutations: List[l.SelectionNode]
    ) -> None:
        if len(deletes) == 0:
            return

        names: _GqlJsonValue = [x.name for x in deletes]

        res = _parse_selection("""
            catalogMultiDeleteSampleByName(input: {}) {
                clientMutationId
            }
            """)
        assert isinstance(res, l.FieldNode)

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value({
            "argExperimentId": self.table.id,
            "argNames": names,
        })

        res.alias = _name_node(f"upd{len(mutations)}")
        res.arguments = tuple([args])

        mutations.append(res)

    # upsert column

    def upsert_column(
        self,
        key: str,
        type: RegistryPythonType,
        *,
        required: bool = False,
    ):
        """Create a column. Support for updating columns is planned.

        Args:
            key: Identifier of the new column.
            type:
                Type of the new column.

                Only a limited set of Python types is currently supported and
                will be expanded over time.

                :class:`latch.registry.types.RegistryPythonType` represents the currently supported types.
            required:
                If true, records without a value for this column are considered invalid.

                Note that an explicit `None` value is different from a missing/empty value.
                `None` is a valid value for an `Optional` (nullable) column marked as required.

        """

        registry_type: Optional[RegistryType] = None
        if type is str:
            registry_type = {"primitive": "string"}
        if type is int:
            registry_type = {"primitive": "integer"}
        if type is float:
            registry_type = {"primitive": "number"}
        if type is date:
            registry_type = {"primitive": "date"}
        if type is datetime:
            registry_type = {"primitive": "datetime"}
        if type is bool:
            registry_type = {"primitive": "boolean"}
        if type is LatchFile:
            registry_type = {"primitive": "blob"}
        if type is LatchDir:
            registry_type = {"primitive": "blob", "metadata": {"nodeType": "dir"}}

        origin = get_origin(type)
        if origin is not None:
            if issubclass(origin, List):
                inner_type = get_args(type)[0]
                inner_type_origin = get_origin(inner_type)

                if inner_type_origin is not None:
                    if issubclass(inner_type_origin, LinkedRecordType):
                        experiment_id = get_args(get_args(inner_type)[0])[0]
                        registry_type = {
                            "array": {
                                "primitive": "link",
                                "experimentId": experiment_id,
                            }
                        }
                    else:
                        raise InvalidColumnTypeError(
                            key, type, "Unsupported list inner type"
                        )
                elif issubclass(inner_type, LatchFile):
                    registry_type = {"array": {"primitive": "blob"}}
                elif issubclass(inner_type, LatchDir):
                    registry_type = {
                        "array": {"primitive": "blob", "metadata": {"nodeType": "dir"}}
                    }
                else:
                    raise InvalidColumnTypeError(
                        key, type, "Unsupported list inner type"
                    )

            if issubclass(origin, LinkedRecordType):
                experiment_id = get_args(get_args(type)[0])[0]
                registry_type = {"primitive": "link", "experimentId": experiment_id}

            if issubclass(origin, RegistryEnumDefinition):
                members = list(get_args(t)[0] for t in get_args(get_args(type)[0]))
                for x in members:
                    if isinstance(x, str):
                        continue
                    raise InvalidColumnTypeError(
                        key, type, f"Enum value {repr(x)} is not a string"
                    )

                registry_type = {
                    "primitive": "enum",
                    "members": members,
                }

        if isinstance(type, Enum):
            members: List[str] = []
            for f in cast(Type[Enum], type):
                if not isinstance(f.value, str):
                    raise InvalidColumnTypeError(
                        key,
                        type,
                        f"Enum value for {repr(f.name)} ({repr(f.value)}) is not a"
                        " string",
                    )

                members.append(f.value)

            registry_type = {
                "primitive": "enum",
                "members": members,
            }

        if registry_type is None:
            raise InvalidColumnTypeError(key, type, "Unsupported type")

        db_type: DBType = {"type": registry_type, "allowEmpty": not required}
        self._record_mutations.append(_TableColumnUpsertData(key, db_type))

    def _add_column_upserts_selection(
        self,
        upserts: List[_TableColumnUpsertData],
        mutations: List[l.SelectionNode],
        vars: Dict[str, Tuple[l.TypeNode, JsonValue]],
    ) -> None:
        if len(upserts) == 0:
            return

        keys: _GqlJsonValue = [x.key for x in upserts]
        types: JsonValue = [cast(JsonValue, x.type) for x in upserts]

        res = _parse_selection("""
            catalogExperimentColumnDefinitionMultiUpsert(input: {}) {
                clientMutationId
            }
        """)
        assert isinstance(res, l.FieldNode)

        argTypesVar = f"upd{len(mutations)}ArgTypes"

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value({
            "argExperimentId": self.table.id,
            "argKeys": keys,
            "argTypes": _var_node(argTypesVar),
        })

        res.alias = _name_node(f"upd{len(mutations)}")
        res.arguments = tuple([args])

        mutations.append(res)
        vars[argTypesVar] = (l.parse_type("[JSON]!"), types)

    def _resolve_upsert_blobs(self) -> None:
        unresolved: List[UnresolvedBlobValue] = []
        for update in self._record_mutations:
            if not isinstance(update, _TableRecordsUpsertData):
                continue

            _get_unresolved_blobs_in_update(update, unresolved)

        if len(unresolved) == 0:
            return

        try:
            res = execute(
                gql.gql("""
                    query ResolvePaths($argPaths: [String]!) {
                        fastLdataMultiResolvePath(argPaths: $argPaths)
                    }
                    """),
                {"argPaths": [data["remote_path"] for data in unresolved]},
            )["fastLdataMultiResolvePath"]
        except gql.transport.exceptions.TransportQueryError as e:
            assert e.errors is not None

            err = e.errors[0]
            raise ValueError(err["message"]) from e

        for i, db_val in enumerate(unresolved):
            data = res[i]

            if data is None:
                raise RegistryTransformerException(
                    f"could not resolve path: {db_val['remote_path']}"
                )

            db_val["ldataNodeId"] = data
            del db_val["remote_path"]

    # transaction

    def commit(self) -> None:
        """Commit this table update transaction.

        May be called multiple times.

        All pending updates are committed with one network request.

        Atomic. The entire transaction either commits or fails with an exception.
        """
        mutations: List[l.SelectionNode] = []
        vars: Dict[str, Tuple[l.TypeNode, JsonValue]] = {}

        if len(self._record_mutations) == 0:
            return

        self._resolve_upsert_blobs()

        def _add_record_data_selection(cur):
            if isinstance(cur[0], _TableRecordsUpsertData):
                self._add_record_upserts_selection(cur, mutations, vars)
            if isinstance(cur[0], _TableRecordsDeleteData):
                self._add_record_deletes_selection(cur, mutations)
            if isinstance(cur[0], _TableColumnUpsertData):
                self._add_column_upserts_selection(cur, mutations, vars)

        cur = [self._record_mutations[0]]
        for mut in self._record_mutations[1:]:
            if isinstance(mut, type(cur[0])):
                cur.append(mut)
                continue

            _add_record_data_selection(cur)
            cur = [mut]

        _add_record_data_selection(cur)

        sel_set = l.SelectionSetNode()
        sel_set.selections = tuple(mutations)

        doc = l.parse("""
            mutation TableUpdate {
                placeholder
            }
        """)

        assert len(doc.definitions) == 1
        mut = doc.definitions[0]

        assert isinstance(mut, l.OperationDefinitionNode)
        mut.selection_set = sel_set

        mut.variable_definitions = tuple(
            _var_def_node(k, t) for k, (t, _) in vars.items()
        )

        # todo(maximsmol): catch errors here and raise appropriate Python exceptions
        # 1. column upsert: already exists
        execute(doc, {k: v for k, (_, v) in vars.items()})

        self.clear()

    def clear(self):
        """Remove pending updates.

        May be called to cancel any pending updates that have not been committed.
        """
        self._record_mutations.clear()
