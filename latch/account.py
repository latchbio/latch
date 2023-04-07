from dataclasses import dataclass, field
from functools import cache
from typing import List, Optional, TypedDict

import gql

from latch.gql._execute import execute
from latch.registry.project import Project


class _CatalogProjectNode(TypedDict):
    id: str
    displayName: str


@dataclass
class _Cache:
    catalog_projects: Optional[List[Project]] = None


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
            document=gql.gql("""
                query accountInfoQuery {
                    accountInfoCurrent {
                        id
                    }
                }
                """),
        )["accountInfoCurrent"]["id"]

        return cls(id=account_id)

    def load(self):
        data = execute(
            gql.gql("""
            query AccountQuery($ownerId: BigInt!) {
                accountInfo(id: $ownerId) {
                    catalogProjectsByOwnerId(
                        condition: {
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
            {"ownerId": self.id},
        )["accountInfo"]
        # todo(maximsmol): deal with nonexistent accounts

        self._cache.catalog_projects = []
        x: _CatalogProjectNode
        for x in data["catalogProjectsByOwnerId"]["nodes"]:
            cur = Project(x["id"])
            cur._cache.display_name = x["displayName"]
            self._cache.catalog_projects.append(cur)

    def list_projects_ext(
        self, *, load_if_missing: bool = False
    ) -> Optional[List[Project]]:
        if self._cache.catalog_projects is None and load_if_missing:
            self.load()

        return self._cache.catalog_projects

    def list_projects(self) -> List[Project]:
        res = self.list_projects_ext(load_if_missing=True)
        assert res is not None
        return res
