from dataclasses import dataclass, field
from functools import cache
from typing import List, Optional, TypedDict

import gql

from latch.gql.execute import execute
from latch.registry.project import Project


class _CatalogProjectNode(TypedDict):
    id: str
    displayName: str


@dataclass
class _Cache:
    catalog_projects: Optional[List[_CatalogProjectNode]] = None


@dataclass(frozen=True)
class Account:
    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str

    @classmethod
    @cache
    def current(cls):
        account_id = execute(
            document=gql.gql(
                """
                query accountInfoQuery {
                    accountInfoCurrent {
                        id
                    }
                }
                """
            ),
        )["accountInfoCurrent"]["id"]

        return cls(id=account_id)

    def load(self):
        query = gql.gql(
            """
            query ProjectsQuery($ownerId: BigInt!) {
                catalogProjects (
                    condition: {
                        ownerId: $ownerId
                        removed: false
                    }
                ) {
                    nodes {
                        id
                        displayName
                    }
                }
            }
            """
        )
        data = execute(query, {"ownerId": self.id})

        self._cache.catalog_projects = data["catalogProjects"]["nodes"]

    def list_projects_ext(
        self, *, load_if_missing: bool = False
    ) -> Optional[List[Project]]:
        if self._cache.catalog_projects is None and load_if_missing:
            self.load()

        xs = self._cache.catalog_projects
        if xs is None:
            return None

        res: List[Project] = []
        for x in xs:
            cur = Project(x["id"])
            cur._cache.display_name = x["displayName"]
            res.append(cur)

        return res

    def list_projects(self) -> List[Project]:
        res = self.list_projects_ext(load_if_missing=True)
        assert res is not None
        return res
