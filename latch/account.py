from dataclasses import dataclass, field
from functools import cache
from typing import List, Literal, Optional, TypedDict, overload

import gql

from latch.gql._execute import execute
from latch.registry.project import Project
from latch_cli.config.user import user_config


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
        if user_config.workspace != "":
            account_id = user_config.workspace
        else:
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

    # list_projects

    @overload
    def list_projects(self, *, load_if_missing: Literal[True] = True) -> List[Project]:
        ...

    @overload
    def list_projects(self, *, load_if_missing: bool) -> Optional[List[Project]]:
        ...

    def list_projects(self, *, load_if_missing: bool = True) -> Optional[List[Project]]:
        if self._cache.catalog_projects is None and load_if_missing:
            self.load()

        return self._cache.catalog_projects
