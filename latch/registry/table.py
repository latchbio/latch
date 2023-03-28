from dataclasses import make_dataclass
from typing import Dict, List, Optional, Type

from latch.gql.execute import execute
from latch.registry.row import Record
from latch.registry.utils import (
    clean,
    to_python_literal,
    to_python_type,
    to_registry_literal,
)


class Table:
    def __init__(self, id: str):
        self.id = id
        self._name: Optional[str] = None
        self._record_type: Type[Record] = None
        self._columns: Optional[List[Dict[str, Dict]]] = None

    def get_display_name(self, load_if_missing=False):
        if self._name is not None:
            return self._name

        self._name = execute(
            """
            query tableNameQuery ($argTableId: BigInt!) {
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
        )["catalogExperiment"]["displayName"]

        return self._name

    def get_columns(self, load_if_missing=False):
        if self._columns is not None:
            return self._columns

        self._columns = execute(
            """
            query tableColumnsQuery ($argTableId: BigInt!) {
                catalogExperiment(id: $argTableId) {
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
        )["catalogExperiment"]["catalogExperimentColumnDefinitionsByExperimentId"][
            "nodes"
        ]

        return self._columns

    def get_record_type(self):
        if self._record_type is not None:
            return self._record_type

        fields = []
        for column in self.get_columns():
            cleaned = clean(column["key"])
            column_db_type = column["type"]

            fields.append(
                (
                    cleaned,
                    to_python_type(
                        column_db_type["type"],
                        cleaned,
                        column_db_type["allowEmpty"],
                    ),
                )
            )

        return make_dataclass(
            clean(f"{self.get_display_name()}_type"),
            fields,
            bases=(Record,),
            frozen=True,
        )

    def list_records(self) -> List[Record]:
        record_type = self.get_record_type()

        records = execute(
            """
            query listRowsQuery($argTableId: BigInt!) {
                catalogExperiment(id: $argTableId) {
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
            {"argTableId": self.id},
        )["catalogExperiment"]["catalogSamplesByExperimentId"]["nodes"]

        output = []

        for record in records:
            record_data = record["catalogSampleColumnDataBySampleId"]["nodes"]
            values = {"id": record["id"], "name": record["name"]}

            try:
                for column in self.get_columns():
                    key = clean(column["key"])
                    typ = column["type"]

                    for data_point in record_data:
                        if clean(data_point["key"]) != key:
                            continue
                        values[key] = to_python_literal(
                            data_point["data"],
                            typ["type"],
                        )
                        break

                    if key not in values:
                        if not typ["allowEmpty"]:
                            raise ValueError(
                                f"no value provided for required column {key} -"
                                " skipping"
                            )
                        values[key] = None
            except ValueError as e:
                print(f"row {record['id']} ({record['name']}) is invalid - skipping")
                print(e)
                continue

            output.append(record_type(**values))

        return output

    def upsert(self, *args: Record):
        valid_records: List[Record] = []
        for record in args:
            for column in self.get_columns():
                cleaned = clean(column["key"])
                if not hasattr(record, cleaned):
                    print(
                        f"row {record.name} is not valid for this table's schema,"
                        f" missing property {cleaned} ({column['key']}) - skipping"
                    )
                    continue
            valid_records.append(record)

        names = []
        for record in valid_records:
            names.append(record.name)

        ids = execute(
            """
            mutation CatalogMultiUpsertSamples($tableId: BigInt!, $names: [String!]!) {
                catalogMultiUpsertSamples(
                    input: { argExperimentId: $tableId, argNames: $names }
                ) {
                    bigInts
                    clientMutationId
                }
            }
            """,
            {
                "tableId": self.id,
                "names": names,
            },
        )["catalogMultiUpsertSamples"]["bigInts"]

        for column in self.get_columns():
            data = []

            for record in valid_records:
                data.append(
                    to_registry_literal(
                        getattr(record, clean(column["key"])),
                        column["type"]["type"],
                    )
                )

            execute(
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
                    "data": data,
                    "key": column["key"],
                    "sampleIds": ids,
                },
            )
