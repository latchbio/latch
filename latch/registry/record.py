from __future__ import annotations  # deal with circular type imports

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Optional

import gql

from latch.gql._execute import execute
from latch.registry.upstream_types.types import DBType
from latch.registry.upstream_types.values import EmptyCell

if TYPE_CHECKING:  # deal with circular type imports
    from latch.registry.types import RecordValue


@dataclass
class _Cache:
    types: Optional[Dict[str, DBType]] = None
    values: Optional[Dict[str, RecordValue]] = None


@dataclass(frozen=True)
class Record:
    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str
    name: str

    def get(self, key: str, default_if_missing: Optional[object] = None):
        return self._cache.values.get(key, default_if_missing)

    def __getitem__(self, key: str):
        if key not in self._cache.values:
            raise KeyError(f"column not found in record {self.id} ({self.name}): {key}")
        return self._cache.values[key]

    @classmethod
    def from_id(cls, id: str):
        # circular import
        from latch.registry.types import InvalidValue
        from latch.registry.utils import to_python_literal

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

        python_values: Dict[str, RecordValue] = {}

        for key, registry_type in column_types_dict.items():
            python_literal = EmptyCell()

            registry_literal = record_data_dict.get(key)
            if registry_literal is not None:
                python_literal = to_python_literal(
                    registry_literal,
                    registry_type["type"],
                )

            python_values[key] = python_literal

        res = cls(
            id=id,
            name=data["name"],
        )
        res._cache.types = column_types_dict
        res._cache.values = python_values
        return res
