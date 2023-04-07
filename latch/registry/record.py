import json
from dataclasses import dataclass
from typing import Dict, Optional

import gql

from latch.gql._execute import execute
from latch.registry.upstream_types.values import EmptyCell


@dataclass(frozen=True)
class Record:
    id: str
    name: str

    _values: Dict[str, object]
    _valid: bool = True

    def get(self, key: str, default_if_missing: Optional[object] = None):
        return self._values.get(key, default_if_missing)

    def __getitem__(self, key: str):
        if key not in self._values:
            raise KeyError(f"column not found in record {self.id} ({self.name}): {key}")
        return self._values[key]

    @classmethod
    def from_id(cls, id: str):
        # circular import
        from latch.registry.utils import InvalidValue, to_python_literal

        data = execute(
            gql.gql("""
                query recordQuery($argRecordId: BigInt!) {
                    catalogSample(id: $argRecordId) {
                        id
                        name
                        experiment {
                            id
                            removed
                            catalogExperimentColumnDefinitionsByExperimentId {
                                nodes {
                                    key
                                    type
                                }
                            }
                        }
                        catalogSampleColumnDataBySampleId {
                            nodes {
                                data
                                key
                            }
                        }
                    }
                }
                """),
            {"argRecordId": id},
        )["catalogSample"]

        if data.get("experiment") is None or data["experiment"]["removed"]:
            return InvalidValue(json.dumps({"sampleId": id}))

        record_data_dict = {
            node["key"]: node["data"]
            for node in data["catalogSampleColumnDataBySampleId"]["nodes"]
        }

        column_types_dict = {
            node["key"]: node["type"]
            for node in data["experiment"][
                "catalogExperimentColumnDefinitionsByExperimentId"
            ]["nodes"]
        }

        python_values: Dict[str, object] = {}
        valid = True

        for key, registry_type in column_types_dict.items():
            python_literal = EmptyCell()
            registry_literal = record_data_dict.get(key)
            if registry_literal is not None:
                python_literal = to_python_literal(
                    registry_literal,
                    registry_type["type"],
                )
            if registry_literal is None and not registry_type["allowEmpty"]:
                valid = False
            if isinstance(python_literal, InvalidValue):
                valid = False

            python_values[key] = python_literal

        return cls(
            id=id,
            name=data["name"],
            _values=python_values,
            _valid=valid,
        )
