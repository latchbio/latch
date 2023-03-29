import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Type, TypedDict

import gql

from latch.gql.execute import execute, get_transport
from latch.registry.record import Record
from latch.registry.utils import (
    clean,
    to_python_literal,
    to_python_type,
    to_registry_literal,
)


class ListRecordsOutputType(TypedDict):
    records: List[Record]
    errors: List[Exception]


class Table:
    def __init__(self, id: str):
        self.id = id
        self._name: Optional[str] = None
        self._record_type: Type[Record] = None
        self._columns: Optional[List[Dict[str, Dict]]] = None
        self._info: Optional[Dict[str, Dict]] = None

    def load(self):
        self.get_display_name(load_if_missing=True)
        self.get_columns(load_if_missing=True)

    def _get_info(self):
        if self._info is not None:
            return self._info

        self._info = execute(
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
                    catalogSamplesByExperimentId(condition: { removed: false }) {
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
                    }
                }
            }
            """,
            variables={"argTableId": self.id},
        )

        return self._info

    def get_display_name(self, load_if_missing=False):
        if not load_if_missing or self._name is not None:
            return self._name

        self._name = self._get_info()["catalogExperiment"]["displayName"]

        return self._name

    def get_columns(self, load_if_missing=False):
        if not load_if_missing or self._columns is not None:
            return self._columns

        self._columns = self._get_info()["catalogExperiment"][
            "catalogExperimentColumnDefinitionsByExperimentId"
        ]["nodes"]

        return self._columns

    def list_records(self) -> Iterator[ListRecordsOutputType]:
        has_next_page = True
        end_cursor = None

        while has_next_page:
            data = execute(
                """
                query tableQuery($argTableId: BigInt!, $argAfter: Cursor) {
                    catalogExperiment(id: $argTableId) {
                        catalogSamplesByExperimentId(
                            condition: { removed: false }
                            first: 50
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
                {"argTableId": self.id, "argAfter": end_cursor},
            )["catalogExperiment"]["catalogSamplesByExperimentId"]

            records = data["nodes"]
            end_cursor = data["pageInfo"]["endCursor"]
            has_next_page = data["pageInfo"]["hasNextPage"]

            output: List[Record] = []
            errors: List[Exception] = []

            for record in records:
                record_data = record["catalogSampleColumnDataBySampleId"]["nodes"]

                values = {}

                try:
                    for column in self.get_columns(load_if_missing=True):
                        key = column["key"]
                        typ = column["type"]

                        for data_point in record_data:
                            if data_point["key"] != key:
                                continue
                            values[key] = to_python_literal(
                                data_point["data"],
                                typ["type"],
                            )
                            break

                        if key not in values:
                            if not typ["allowEmpty"]:
                                raise ValueError(
                                    f"no value provided for required column {key}"
                                )
                            values[key] = None
                except ValueError as e:
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
                        _value_dict=values,
                    )
                )

            yield {"records": output, "errors": errors}

    def update(self):
        return TableUpdater(self)


@dataclass
class TableUpdate:
    table: Table

    async def execute(self, session: gql.Client):
        ...


# todo(ayush): "DeleteRecordUpdate", "UpsertColumnUpdate", "DeleteColumnUpdate"
@dataclass
class UpsertRecordUpdate(TableUpdate):
    name: str
    data: Dict[str, Any]

    async def execute(self, session: gql.Client):
        record_id = await session.execute(
            gql.gql(
                """
                mutation CatalogMultiUpsertSamples($tableId: BigInt!, $names: [String!]!) {
                    catalogMultiUpsertSamples(
                        input: { argExperimentId: $tableId, argNames: $names }
                    ) {
                        bigInts
                        clientMutationId
                    }
                }
                """
            ),
            {
                "tableId": self.table.id,
                "names": [self.name],
            },
        )["catalogMultiUpsertSamples"]["bigInts"][0]

        for column in self.table.get_columns():
            literal = None
            if column["key"] not in self.data:
                if not column["type"]["allowEmpty"]:
                    raise ValueError(
                        f"unable to upsert record {self.name} as column"
                        f" {column['key']} is missing a value"
                    )
            else:
                literal = to_registry_literal(
                    self.data[column["key"]],
                    column["type"]["type"],
                )

            await session.execute(
                """
                mutation CatalogMultiUpsertColumnDatum(
                    $data: [JSON]!
                    $key: String!
                    $sampleIds: [BigInt!]!
                ) {
                    catalogMultiUpsertColumnSampleData(
                        input: { argSampleIds: $sampleIds, argKey: $key, argData: $data }
                    ) {
                        clientMutationId
                    }
                }
                """,
                {
                    "data": [literal],
                    "key": column["key"],
                    "sampleIds": [record_id],
                },
            )


class TableUpdater:
    def __init__(self, table: Table):
        self.table = table
        self.updates: List[TableUpdate] = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if type is not None or value is not None or tb is not None:
            return False
        self.commit()

    def upsert_record(self, record_name: str, column_data: Dict[str, Any]):
        self.updates.append(
            UpsertRecordUpdate(
                self.table,
                record_name,
                column_data,
            )
        )

    def commit(self):
        async def helper():
            async with gql.Client(transport=get_transport()) as session:
                for update in self.updates:
                    await update.execute(session)

        asyncio.run(helper())
