import asyncio
import json
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Type, TypedDict

import gql
import graphql.language as l
import graphql.type as t
import graphql.utilities as u

from latch.gql.execute import execute, get_transport
from latch.registry.record import Record
from latch.registry.types import RegistryDBValue, registry_empty_cell
from latch.registry.utils import (
    RegistryTransformerException,
    to_python_literal,
    to_registry_literal,
)


@dataclass(frozen=True)
class ListRecordsOutput:
    records: List[Record]
    errors: List[Exception]


@dataclass
class Table:
    id: str

    def __post_init__(self):
        self._display_name: Optional[str] = None
        self._columns: Optional[List[Dict[str, Dict]]] = None
        self._data: Optional[Dict[str, Dict]] = None

    def load(self):
        self._load()

    def _load(self):
        if self._data is not None:
            return self._data

        # todo(ayush): paginate column defs too?
        self._data = execute(
            """
            query tableQuery ($argTableId: BigInt!) {
                catalogExperiment(id: $argTableId) {
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
            """,
            variables={"argTableId": self.id},
        )
        self._display_name = self._data["catalogExperiment"]["displayName"]
        self._columns = self._data["catalogExperiment"][
            "catalogExperimentColumnDefinitionsByExperimentId"
        ]["nodes"]

        return self._data

    def get_display_name(self, load_if_missing=False):
        if not load_if_missing or self._display_name is not None:
            return self._display_name

        self._load()

        return self._display_name

    def get_columns(self, load_if_missing=False):
        if not load_if_missing or self._columns is not None:
            return self._columns

        self._load()

        return self._columns

    def list_records(self, *ignored_args, page_size=100) -> Iterator[ListRecordsOutput]:
        has_next_page = True
        end_cursor = None

        while has_next_page:
            data = execute(
                """
                query tableQuery($argTableId: BigInt!, $argAfter: Cursor, $argPageSize: Int) {
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
                """,
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
                    for column in self.get_columns(load_if_missing=True):
                        key = column["key"]
                        typ = column["type"]

                        data_point = record_data.get(key)
                        if data_point is not None:
                            values[key] = to_python_literal(
                                data_point,
                                typ["type"],
                            )

                        if key not in values:
                            if not typ["allowEmpty"]:
                                valid = False
                            values[key] = registry_empty_cell
                except RegistryTransformerException as e:
                    # todo(ayush): raise immediately and prompt for confirmation
                    # if the user wants the rest of the page to be processed?
                    errors.append(
                        ValueError(
                            f"record {record['id']} ({record['name']}) is invalid: {e}"
                        )
                    )
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

    @abstractmethod
    async def get_document(self) -> l.DocumentNode:
        raise NotImplementedError


# todo(ayush): "DeleteRecordUpdate", "UpsertColumnUpdate", "DeleteColumnUpdate"
@dataclass(frozen=True)
class UpsertRecordUpdate(TableUpdate):
    name: str
    data: Dict[str, object]
    op_index: int

    def get_document(self) -> str:
        errors: Dict[str, str] = {}

        keys: List[str] = []
        registry_literal_strings: List[str] = []

        columns = self.table.get_columns(load_if_missing=True)
        column_dict = {column["key"]: column["type"] for column in columns}

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

        return gql.gql(
            f"""
            mutation UpsertSampleWithData {{
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
            }}
            """
        )


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

        execute(batched_document)
