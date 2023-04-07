from __future__ import annotations  # avoid circular type imports

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, TypedDict, overload

import gql

from latch.gql._execute import execute
from latch.registry.upstream_types.types import DBType
from latch.registry.upstream_types.values import DBValue

if TYPE_CHECKING:  # avoid circular type imports
    from latch.registry.types import Column, RecordValue


class _ColumnDefinition(TypedDict("_ColumnDefinitionReserved", {"def": DBValue})):
    key: str
    type: DBType


class _ColumnDefinitionConnection(TypedDict):
    nodes: List[_ColumnDefinition]


class _CatalogExperiment(TypedDict):
    catalogExperimentColumnDefinitionsByExperimentId: _ColumnDefinitionConnection


class _ColumnDataNode(TypedDict):
    key: str
    data: DBValue


class _ColumnDataConnection(TypedDict):
    nodes: List[_ColumnDataNode]


class _CatalogSample(TypedDict):
    id: str
    name: str
    experiment: _CatalogExperiment
    catalogSampleColumnDataBySampleId: _ColumnDataConnection


@dataclass
class _Cache:
    name: Optional[str] = None
    columns: Optional[Dict[str, Column]] = None
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

    def load(self):
        # avoid circular type imports
        from latch.registry.types import Column, InvalidValue
        from latch.registry.utils import to_python_literal, to_python_type

        data: _CatalogSample = execute(
            gql.gql("""
            query RecordQuery($id: BigInt!) {
                catalogSample(id: $id) {
                    id
                    name
                    catalogSampleColumnDataBySampleId {
                        nodes {
                            key
                            data
                        }
                    }
                    experiment {
                        catalogExperimentColumnDefinitionsByExperimentId {
                            nodes {
                                type
                                key
                                def
                            }
                        }
                    }
                }
            }
            """),
            {"id": self.id},
        )["catalogSample"]
        # todo(maximsmol): deal with nonexistent records

        self._cache.name = data["name"]

        typeNodes = data["experiment"][
            "catalogExperimentColumnDefinitionsByExperimentId"
        ]["nodes"]
        # fixme(maximsmol): deal with defaults
        self._cache.columns = {
            n["key"]: Column(n["key"], to_python_type(n["type"]["type"]), n["type"])
            for n in typeNodes
        }

        valNodes = data["catalogSampleColumnDataBySampleId"]["nodes"]
        colVals = {n["key"]: n["data"] for n in valNodes}

        vals: Dict[str, RecordValue] = {}
        for k, v in colVals.items():
            # todo(maximsmol): allow creating records with mismatching types
            vals[k] = to_python_literal(v, self._cache.columns[k].upstream_type["type"])

        for k, c in self._cache.columns.items():
            if k in vals:
                continue

            if not c.upstream_type["allowEmpty"]:
                vals[k] = InvalidValue("")

        self._cache.values = vals

    # get_name

    @overload
    def get_name(self, *, load_if_missing: Literal[True] = True) -> str:
        ...

    @overload
    def get_name(self, *, load_if_missing: bool) -> Optional[str]:
        ...

    def get_name(self, *, load_if_missing: bool = True) -> Optional[str]:
        if self._cache.name is None and load_if_missing:
            self.load()

        return self._cache.name

    # get_columns

    @overload
    def get_columns(
        self,
        *,
        load_if_missing: Literal[True] = True,
    ) -> Dict[str, Column]:
        ...

    @overload
    def get_columns(
        self,
        *,
        load_if_missing: bool,
    ) -> Optional[Dict[str, Column]]:
        ...

    def get_columns(
        self,
        *,
        load_if_missing: bool = True,
    ) -> Optional[Dict[str, Column]]:
        if self._cache.columns is None and load_if_missing:
            self.load()

        return self._cache.columns

    # get_values

    @overload
    def get_values(
        self,
        *,
        load_if_missing: Literal[True] = True,
    ) -> Dict[str, RecordValue]:
        ...

    @overload
    def get_values(
        self,
        *,
        load_if_missing: bool,
    ) -> Optional[Dict[str, RecordValue]]:
        ...

    def get_values(
        self,
        *,
        load_if_missing: bool = True,
    ) -> Optional[Dict[str, RecordValue]]:
        if self._cache.values is None and load_if_missing:
            self.load()

        return self._cache.values

    def __repr__(self):
        if self._cache.name is None:
            res = f"Record(id={self.id})"
        else:
            res = f"Record(id={self.id}, name={self._cache.name})"

        if self._cache.values is None:
            return res

        return res + repr({k: v for k, v in self._cache.values.items()})

    def __str__(self):
        if self._cache.name is None:
            res = f"Record(id={self.id})"
        else:
            res = f"Record(id={self.id}, name={self._cache.name})"

        if self._cache.values is None:
            return res

        return res + str({k: v for k, v in self._cache.values.items()})
