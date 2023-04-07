from dataclasses import dataclass, field
from typing import List, Optional, TypedDict

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

    def get_display_name_ext(self, *, load_if_missing: bool = False) -> Optional[str]:
        if self._cache.display_name is None and load_if_missing:
            self.load()

        return self._cache.display_name

    def get_display_name(self) -> str:
        res = self.get_display_name_ext(load_if_missing=True)
        assert res is not None
        return res

    def list_tables_ext(
        self, *, load_if_missing: bool = False
    ) -> Optional[List[Table]]:
        if self._cache.experiments is None and load_if_missing:
            self.load()

        return self._cache.experiments

    def list_tables(self):
        res = self.list_tables_ext(load_if_missing=True)
        assert res is not None
        return res

    def __str__(self):
        name = self.get_display_name_ext(load_if_missing=False)
        if name is not None:
            return f"Project({repr(name)})"

        return repr(self)
