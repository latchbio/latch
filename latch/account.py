from dataclasses import dataclass, field
from functools import cache
from typing import List, Literal, Optional, TypedDict, overload

import gql
from typing_extensions import Self

from latch.gql._execute import execute
from latch.registry.project import Project
from latch.registry.table import Table
from latch_cli.config.user import user_config


class _CatalogExperiment(TypedDict):
    id: str
    displayName: str


class _CatalogExperimentConnection(TypedDict):
    nodes: List[_CatalogExperiment]


class _CatalogProjectNode(TypedDict):
    id: str
    displayName: str

    catalogExperimentsByProjectId: _CatalogExperimentConnection


class _CatalogProjectConnection(TypedDict):
    nodes: List[_CatalogProjectNode]


class _Account(TypedDict):
    catalogProjectsByOwnerId: _CatalogProjectConnection


@dataclass
class _Cache:
    catalog_projects: Optional[List[Project]] = None


@dataclass(frozen=True)
class Account:
    """User or team workspace. Can be used to fetch related resources.

    :meth:`current` is the typical way of getting an :class:`Account`.

    If the current request signer (CLI user or execution context)
    lacks permissions to fetch some information, the corresponding operations
    will act as if the information does not exist. Update operations will usually
    produce errors.
    """

    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str
    """Unique identifier."""

    @classmethod
    @cache
    def current(cls) -> Self:
        """Get current account.

        In an execution context, this is the workspace in which the execution
        was run.

        In the CLI context (when running `latch` commands) this is the
        current setting of `latch workspace`, which defaults to the user's personal
        workspace.

        Returns:
            Current account.
        """
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

    def load(self) -> None:
        """(Re-)populate this account instance's cache.

        Future calls to most getters will return immediately without making a network request.

        Always makes a network request.
        """
        data: _Account = execute(
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

                            catalogExperimentsByProjectId {
                                nodes {
                                    id
                                    displayName
                                }
                            }
                        }
                    }
                }
            }
            """),
            {"ownerId": self.id},
        )["accountInfo"]
        # todo(maximsmol): deal with nonexistent accounts

        self._cache.catalog_projects = []
        for x in data["catalogProjectsByOwnerId"]["nodes"]:
            cur = Project(x["id"])
            self._cache.catalog_projects.append(cur)

            cur._cache.display_name = x["displayName"]

            cur._cache.tables = []
            for t in x["catalogExperimentsByProjectId"]["nodes"]:
                table = Table(t["id"])
                cur._cache.tables.append(table)

                table._cache.display_name = x["displayName"]

    # list_registry_projects

    @overload
    def list_registry_projects(
        self, *, load_if_missing: Literal[True] = True
    ) -> List[Project]:
        ...

    @overload
    def list_registry_projects(
        self, *, load_if_missing: bool
    ) -> Optional[List[Project]]:
        ...

    def list_registry_projects(
        self, *, load_if_missing: bool = True
    ) -> Optional[List[Project]]:
        """List Registry projects owned by this workspace.

        Args:
            load_if_missing:
                If true, :meth:`load` the project list if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Projects owned by this workspace.
        """
        if self._cache.catalog_projects is None and load_if_missing:
            self.load()

        return self._cache.catalog_projects
