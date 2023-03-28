from dataclasses import make_dataclass
from typing import List

from dacite import Config, from_dict

from latch.gql.execute import execute
from latch.registry.row import Row
from latch.registry.utils import (
    clean,
    to_python_literal,
    to_python_type,
    to_registry_literal,
)


class Table:
    def __init__(self, table_id: str):
        self.id = table_id
        self._name = None
        self._row_type = None
        self._columns = None

    @property
    def name(self):
        if self._name is not None:
            return self._name

        self._name = execute(
            """
            query tableNameQuery ($argTableId: BigInt!) {
                catalogExperiment(id: $argTableId) {
                    displayName
                }
            }
            """,
            variables={
                "argTableId": self.id,
            },
        )["catalogExperiment"]["displayName"]

        return self._name

    @property
    def columns(self):
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

    @property
    def row_type(self):
        if self._row_type is not None:
            return self._row_type

        fields = []
        for column in self.columns:
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
            clean(f"{self.name}_type"),
            fields,
            bases=(Row,),
            frozen=True,
        )

    def list_rows(self) -> List[Row]:
        rows = execute(
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

        for row in rows:
            row_data = row["catalogSampleColumnDataBySampleId"]["nodes"]
            values = {"id": row["id"], "name": row["name"]}

            try:
                for column in self.columns:
                    key = clean(column["key"])
                    typ = column["type"]

                    for data_point in row_data:
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
                print(f"row {row['id']} ({row['name']}) is invalid - skipping")
                print(e)
                continue

            output.append(
                from_dict(
                    self.row_type,
                    values,
                    Config(
                        check_types=False
                    ),  # this library is dumb and doesn't understand Optionals
                )
            )

        return output

    def upsert(self, *args: Row):
        valid_rows: List[Row] = []
        for row in args:
            for column in self.columns:
                cleaned = clean(column["key"])
                if not hasattr(row, cleaned):
                    print(
                        f"row {row.name} is not valid for this table's schema, missing"
                        f" property {cleaned} ({column['key']}) - skipping"
                    )
                    continue
            valid_rows.append(row)

        names = []
        for row in valid_rows:
            names.append(row.name)

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

        for column in self.columns:
            data = []

            for row in valid_rows:
                data.append(
                    to_registry_literal(
                        getattr(row, clean(column["key"])),
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
