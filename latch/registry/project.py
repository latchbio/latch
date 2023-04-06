from dataclasses import dataclass, field
from typing import List, Optional, TypedDict

import gql

from latch.gql.execute import execute
from latch.registry.table import Table


class _CatalogExperimentNode(TypedDict):
    id: str
    displayName: str


@dataclass
class _Cache:
    display_name: Optional[str] = None
    experiments: Optional[List[_CatalogExperimentNode]] = None


@dataclass
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
            document=gql.gql(
                """
                query ProjectQuery ($id: BigInt!) {
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
                """
            ),
            variables={"id": self.id},
        )["catalogProject"]
        if data is None:
            raise

        self._cache.display_name = data["displayName"]
        self._cache.experiments = data["catalogExperimentsByProjectId"]["nodes"]

    def get_display_name_ext(self, *, load_if_missing: bool = False) -> Optional[str]:
        if self._cache.display_name is None and load_if_missing:
            self.load()

        return self._cache.display_name

    def get_display_name(self):
        res = self.get_display_name_ext(load_if_missing=True)
        assert res is not None
        return res

    def list_tables_ext(
        self, *, load_if_missing: bool = False
    ) -> Optional[List[Table]]:
        if self._cache.experiments is None and load_if_missing:
            self.load()

        xs = self._cache.experiments
        if xs is None:
            return None

        res: List[Table] = []
        for x in xs:
            cur = Table(x["id"])
            cur._display_name = x["displayName"]
            res.append(cur)

        return res

    def list_tables(self):
        res = self.list_tables_ext(load_if_missing=True)
        assert res is not None
        return res

    def __str__(self):
        name = self.get_display_name_ext(load_if_missing=False)
        if name is not None:
            return f"Project({repr(name)})"

        return repr(self)
