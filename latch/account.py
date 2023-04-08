from dataclasses import dataclass, field
from functools import cache
from typing import List, Literal, Optional, TypedDict, overload

import gql
from typing_extensions import Self

from latch.gql._execute import execute
from latch.registry.project import Project
from latch_cli.config.user import user_config


class _CatalogProjectNode(TypedDict):
    id: str
    displayName: str


@dataclass
class _Cache:
    """Internal cache class to organize information for a `Account`."""

    catalog_projects: Optional[List[Project]] = None


@dataclass(frozen=True)
class Account:
    """A python representation of an Account on Latch

    This class mirrors an Account on Latch and provides a method to list the
    Projects in the underlying Account.

    `Account`s can be instantiated directly by ID or by using the constructor
    method `Account.current()`. A call to `Account.current()` will return an
    `Account` that corresponds to the workspace in which the code was run. In an
    execution context, this is the workspace in which the execution was run. In
    a local context, e.g. through `latch develop`, this is the current setting
    of `latch workspace`, defaulting to the user's personal workspace if no
    setting is found.

    Even though an `Account` can be instantiated with any ID, its methods will
    only work if the user running them has access to the underlying Account. If
    a user were to create an `Account` they do not have access to, that
    `Account` would be completely impotent.

    Attributes:
        id:
            The ID of the underlying Account as a string.
        _cache:
            A private cache for values that need to be queried over the network,
            should not be accessed directly.
    """

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
    def current(cls) -> Self:
        """Factory for `Account`.

        Will return an `Account` that corresponds to the workspace in which the
        code was run. This is a convenience method so that users don't have to
        worry about managing Account IDs directly.

        In an execution context, this is the workspace in which the execution
        was run. In a local context, e.g. through `latch develop`, this is the
        current setting of `latch workspace`, defaulting to the user's personal
        workspace if no setting is found.

        Returns:
            An `Account` describing the current workspace.

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
        """Loads all properties at once.

        Performs a GraphQL request and uses the results to populate the calling
        Accounts's cache. This is called by `.list_projects()` when
        `load_if_missing` is set to True (the default).
        """
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
        """Returns all Registry Projects in the underlying Account.

        This function returns a list of `Project` objects, each of which is a
        Project in the underlying Account. By default this will call `.load()`
        if the Projects have not been queried yet, but this behavior can be
        disabled by setting `load_if_missing` to False.

        Args:
            load_if_missing:
                Keyword-only. Controls whether or not a call to `.load()` will
                be made if the value has not already been queried.
                True by default.

        Returns:
            A list of `Project`s.

        """

        if self._cache.catalog_projects is None and load_if_missing:
            self.load()

        return self._cache.catalog_projects
