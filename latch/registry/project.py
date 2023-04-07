from dataclasses import dataclass, field
from typing import List, Literal, Optional, TypedDict, overload

import gql

from latch.gql._execute import execute
from latch.registry.table import Table


class _CatalogExperimentNode(TypedDict):
    id: str
    displayName: str


@dataclass
class _Cache:
    display_name: Optional[str] = None
    experiments: Optional[List[Table]] = None


@dataclass(frozen=True)
class Project:
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
            document=gql.gql("""
                query ProjectQuery($id: BigInt!) {
                    catalogProject(id: $id) {
                        id
                        displayName
                        catalogExperimentsByProjectId (
                            condition: {
                                projectId: $id
                                removed: false
                            }
                        ) {
                            nodes {
                                id
                                displayName
                            }
                        }
                    }
                }
                """),
            variables={"id": self.id},
        )["catalogProject"]
        # todo(maximsmol): deal with nonexistent projects

        self._cache.display_name = data["displayName"]

        self._cache.experiments = []
        experiments: List[_CatalogExperimentNode] = data[
            "catalogExperimentsByProjectId"
        ]["nodes"]
        for x in experiments:
            cur = Table(x["id"])
            cur._cache.display_name = x["displayName"]
            self._cache.experiments.append(cur)

    # get_display_name

    @overload
    def get_display_name(self, *, load_if_missing: Literal[True] = True) -> str:
        ...

    @overload
    def get_display_name(self, *, load_if_missing: bool) -> Optional[str]:
        ...

    def get_display_name(self, *, load_if_missing: bool = True) -> Optional[str]:
        if self._cache.display_name is None and load_if_missing:
            self.load()

        return self._cache.display_name

    # list_tables

    @overload
    def list_tables(self, *, load_if_missing: Literal[True] = True) -> List[Table]:
        ...

    @overload
    def list_tables(self, *, load_if_missing: bool) -> Optional[List[Table]]:
        ...

    def list_tables(self, *, load_if_missing: bool = True) -> Optional[List[Table]]:
        if self._cache.experiments is None and load_if_missing:
            self.load()

        return self._cache.experiments

    def __str__(self):
        name = self.get_display_name(load_if_missing=False)
        if name is not None:
            return f"Project({repr(name)})"

        return repr(self)
