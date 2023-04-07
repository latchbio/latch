import json
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Type, TypedDict, Union

import gql
import graphql.language as l
import graphql.type as t
import graphql.utilities as u
from typing_extensions import override

from latch.gql.execute import execute
from latch.registry.record import Record
from latch.registry.upstream_types.types import DBType
from latch.registry.upstream_types.values import EmptyCell
from latch.registry.utils import (
    RegistryPythonValue,
    RegistryTransformerException,
    to_python_literal,
    to_python_type,
    to_registry_literal,
)


@dataclass(frozen=True)
class ListRecordsOutput:
    records: List[Record]
    errors: List[Exception]


class _ColumnNode(TypedDict):
    key: str
    type: DBType


@dataclass(frozen=True)
class Column:
    key: str
    type: Union[Type[RegistryPythonValue], Type[Union[RegistryPythonValue, EmptyCell]]]
    upstream_type: DBType


@dataclass
class _Cache:
    display_name: Optional[str] = None
    columns: Optional[List[Column]] = None


@dataclass(frozen=True)
class Table:
    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str

    def load(self):
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
                            }
                        }
                    }
                }
                """),
            variables={"id": self.id},
        )["catalogExperiment"]
        # todo(maximsmol): deal with nonexistent tables

        self._cache.display_name = data["displayName"]

        self._cache.columns = []
        columns: List[_ColumnNode] = data[
            "catalogExperimentColumnDefinitionsByExperimentId"
        ]["nodes"]
        for x in columns:
            py_type = to_python_type(x["type"]["type"])
            if x["type"]["allowEmpty"]:
                py_type = Union[py_type, EmptyCell]

            cur = Column(x["key"], py_type, x["type"])

            self._cache.columns.append(cur)

    def get_display_name_ext(self, *, load_if_missing: bool = False) -> Optional[str]:
        if self._cache.display_name is None and load_if_missing:
            self.load()

        return self._cache.display_name

    def get_display_name(self) -> str:
        res = self.get_display_name_ext(load_if_missing=True)
        assert res is not None
        return res

    def get_columns_ext(
        self, *, load_if_missing: bool = False
    ) -> Optional[List[Column]]:
        if self._cache.columns is None and load_if_missing:
            self.load()

        return self._cache.columns

    def get_columns(self) -> List[Column]:
        res = self.get_columns_ext(load_if_missing=True)
        assert res is not None
        return res

    def list_records(self, *, page_size: int = 100) -> Iterator[ListRecordsOutput]:
        has_next_page = True
        end_cursor = None

        while has_next_page:
            # todo(ayush): switch this to a paginated ver of
            # app_public.catalog_experiment_all_samples to get around RLS
            # performance issues
            data = execute(
                gql.gql("""
                    query TableQuery($argTableId: BigInt!, $argAfter: Cursor, $argPageSize: Int) {
                        catalogExperiment(id: $argTableId) {
                            catalogSamplesByExperimentId(
                                condition: { removed: false }
                                first: $argPageSize
                                after: $argAfter
                            ) {
                                nodes {
                                    id
                                    name
                                    catalogSampleColumnDataBySampleId {
                                        nodes {
                                            data
                                            key
                                        }
                                    }
                                }
                                pageInfo {
                                    endCursor
                                    hasNextPage
                                }
                            }
                        }
                    }
                    """),
                {
                    "argTableId": self.id,
                    "argAfter": end_cursor,
                    "argPageSize": page_size,
                },
            )["catalogExperiment"]["catalogSamplesByExperimentId"]

            records = data["nodes"]
            end_cursor = data["pageInfo"]["endCursor"]
            has_next_page = data["pageInfo"]["hasNextPage"]

            output: List[Record] = []
            errors: List[Exception] = []

            for record in records:
                record_data = {
                    node["key"]: node["data"]
                    for node in record["catalogSampleColumnDataBySampleId"]["nodes"]
                }

                values: Dict[str, object] = {}
                valid = True

                try:
                    for column in self.get_columns():
                        key = column.key
                        typ = column.upstream_type

                        data_point = record_data.get(key)
                        if data_point is not None:
                            values[key] = to_python_literal(
                                data_point,
                                typ["type"],
                            )

                        if key not in values:
                            if not typ["allowEmpty"]:
                                valid = False
                            values[key] = EmptyCell()
                except RegistryTransformerException as e:
                    # todo(ayush): raise immediately and prompt for confirmation
                    # if the user wants the rest of the page to be processed?

                    err = ValueError(
                        f"record {record['id']} ({record['name']}) is invalid"
                    )
                    # emulating `raise ... from e`
                    err.__context__ = e
                    err.__cause__ = e
                    errors.append(err)
                    continue

                output.append(
                    Record(
                        id=record["id"],
                        name=record["name"],
                        _values=values,
                        _valid=valid,
                    )
                )

            yield ListRecordsOutput(records=output, errors=errors)

    def update(self):
        return TableUpdater(self)


@dataclass(frozen=True)
class TableUpdate:
    table: Table

    # todo(maximsmol): switch to using l.DocumentNode
    @abstractmethod
    def get_document(self) -> str:
        raise NotImplementedError()


# todo(ayush): "DeleteRecordUpdate", "UpsertColumnUpdate", "DeleteColumnUpdate"
@dataclass(frozen=True)
class UpsertRecordUpdate(TableUpdate):
    name: str
    data: Dict[str, object]
    op_index: int

    @override
    def get_document(self) -> str:
        errors: Dict[str, str] = {}

        keys: List[str] = []
        registry_literal_strings: List[str] = []

        columns = self.table.get_columns()
        column_dict = {column.key: column.type for column in columns}

        for key, python_literal in self.data.items():
            try:
                registry_type = column_dict.get(key)
                registry_literal = to_registry_literal(
                    python_literal, registry_type["type"]
                )
            except RegistryTransformerException as e:
                errors[key] = f"unable to generate registry literal for {key}: {e}"
                continue

            keys.append(key)
            registry_literal_strings.append(json.dumps(registry_literal))

        arg_experiment_id = l.print_ast(
            u.ast_from_value(self.table.id, t.GraphQLString)
        )
        arg_name = l.print_ast(u.ast_from_value(self.name, t.GraphQLString))
        arg_keys = l.print_ast(u.ast_from_value(keys, t.GraphQLList(t.GraphQLString)))
        arg_data = l.print_ast(
            u.ast_from_value(registry_literal_strings, t.GraphQLList(t.GraphQLString))
        )

        return f"""
        m{self.op_index}: catalogUpsertSampleWithData(
            input: {{
                argExperimentId: {arg_experiment_id}
                argName: {arg_name}
                argKeys: {arg_keys}
                argData: {arg_data}
            }}
        ) {{
            clientMutationId
        }}
        """


@dataclass(frozen=True)
class TableUpdater:
    table: Table
    _updates: List[TableUpdate] = field(default_factory=list)

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if type is not None or value is not None or tb is not None:
            return False
        self.commit()

    def upsert_record(self, record_name: str, column_data: Dict[str, Any]):
        self._updates.append(
            UpsertRecordUpdate(
                self.table,
                record_name,
                column_data,
                len(self._updates),
            )
        )

    def commit(self):
        documents: List[str] = []

        while len(self._updates) > 0:
            update = self._updates.pop()
            documents.append(update.get_document())

        documents.reverse()

        batched_document = f"""
            mutation UpsertSampleWithData {{
                {"".join(documents)}
            }}
        """

        execute(gql.gql(batched_document))
