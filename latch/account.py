from dataclasses import dataclass, field
from functools import cache
from typing import List, Optional

import gql

from latch.gql.execute import execute
from latch.registry.project import Project


@dataclass
class _AccountCache:
    # todo(maximsmol): gql type generation
    catalogProjects: Optional[List[object]] = None


@dataclass(frozen=True)
class Account:
    _cache: _AccountCache = field(
        default_factory=lambda: _AccountCache(),
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
            query ProjectsQuery ($argOwnerId: BigInt!) {
                catalogProjects (
                    condition: {
                        ownerId: $argOwnerId
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
        data = execute(query, {"argOwnerId": self.id})

        self._cache.catalogProjects = data["catalogProjects"]["nodes"]

    def list_projects_ext(
        self, *, load_if_missing: bool = False
    ) -> Optional[List[Project]]:
        if self._cache.catalogProjects is None and load_if_missing:
            self.load()

        xs = self._cache.catalogProjects
        if xs is None:
            return None

        return [Project(x["id"]) for x in xs]

    def list_projects(self) -> List[Project]:
        res = self.list_projects_ext(load_if_missing=True)
        assert res is not None
        return res
