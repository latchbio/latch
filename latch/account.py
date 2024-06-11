from contextlib import contextmanager
from dataclasses import dataclass, field

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from typing import Iterator, List, Literal, Optional, TypedDict, Union, overload

import gql
import graphql.language as l
from latch_sdk_gql.execute import execute
from latch_sdk_gql.utils import _GqlJsonValue, _json_value, _name_node, _parse_selection
from typing_extensions import Self, TypeAlias

from latch.registry.project import Project
from latch.registry.table import Table
from latch.utils import NotFoundError, current_workspace


class AccountNotFoundError(NotFoundError): ...


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
        current setting of `latch workspace`, which defaults to the user's default
        workspace.

        Returns:
            Current account.
        """
        return cls(id=current_workspace())

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

                            catalogExperimentsByProjectId(condition: {removed: false}) {
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

        if data is None:
            raise AccountNotFoundError(
                f"account does not exist or you lack permissions: id={self.id}"
            )

        self._cache.catalog_projects = []
        for x in data["catalogProjectsByOwnerId"]["nodes"]:
            cur = Project(x["id"])
            self._cache.catalog_projects.append(cur)

            cur._cache.display_name = x["displayName"]

            cur._cache.tables = []
            for t in x["catalogExperimentsByProjectId"]["nodes"]:
                table = Table(t["id"])
                cur._cache.tables.append(table)

                table._cache.display_name = t["displayName"]

    # list_registry_projects

    @overload
    def list_registry_projects(
        self, *, load_if_missing: Literal[True] = True
    ) -> List[Project]: ...

    @overload
    def list_registry_projects(
        self, *, load_if_missing: bool
    ) -> Optional[List[Project]]: ...

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

    @contextmanager
    def update(self, *, reload_on_commit: bool = True) -> Iterator["AccountUpdate"]:
        """Start an update transaction.

        The transaction will commit when the context manager closes unless an error occurs.

        No changes will occur until the transaction commits.

        The transaction can be cancelled by running :meth:`AccountUpdate.clear`
        before closing the context manager.

        Args:
            reload_on_commit:
                If true, :meth:`load` this account after the transaction commits.

        Returns:
            Context manager for the new transaction.
        """

        upd = AccountUpdate(self)
        yield upd
        upd.commit()

        if reload_on_commit:
            self.load()

    def __repr__(self):
        return f"Account(id={self.id})"

    def __str__(self):
        return repr(self)


@dataclass(frozen=True)
class _AccountRegistryProjectsUpsertData:
    display_name: str


@dataclass(frozen=True)
class _AccountRegistryProjectsDeleteData:
    id: str


_AccountMutationsData: TypeAlias = Union[
    _AccountRegistryProjectsUpsertData, _AccountRegistryProjectsDeleteData
]


@dataclass(frozen=True)
class AccountUpdate:
    _mutations: List[_AccountMutationsData] = field(
        default_factory=list,
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    account: Account

    # upsert registry project

    def upsert_registry_project(self, display_name: str):
        """Upsert a registry project.

        Not idempotent. Two calls with the same args will create two projects.

        Args:
            display_name: Display name of the new project.
        """
        self._mutations.append(_AccountRegistryProjectsUpsertData(display_name))

    def _add_registry_projects_upsert_selection(
        self,
        upserts: List[_AccountRegistryProjectsUpsertData],
        mutations: List[l.SelectionNode],
    ):
        if len(upserts) == 0:
            return

        display_names: _GqlJsonValue = [x.display_name for x in upserts]

        res = _parse_selection("""
            catalogMultiCreateProjects(input: {}) {
                clientMutationId
            }
            """)
        assert isinstance(res, l.FieldNode)

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value({
            "argOwnerId": self.account.id,
            "argDisplayNames": display_names,
        })

        res.alias = _name_node(f"upd{len(mutations)}")
        res.arguments = tuple([args])

        mutations.append(res)

    # delete registry project

    def delete_registry_project(self, id: str):
        """Delete a registry project.

        Args:
            id: The ID of the target project.
        """
        self._mutations.append(_AccountRegistryProjectsDeleteData(id))

    def _add_registry_projects_delete_selection(
        self,
        deletes: List[_AccountRegistryProjectsDeleteData],
        mutations: List[l.SelectionNode],
    ):
        if len(deletes) == 0:
            return

        ids: _GqlJsonValue = [x.id for x in deletes]

        res = _parse_selection("""
            catalogMultiDeleteProjects(input: {}) {
                clientMutationId
            }
            """)
        assert isinstance(res, l.FieldNode)

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value({
            "argIds": ids,
        })

        res.alias = _name_node(f"upd{len(mutations)}")
        res.arguments = tuple([args])

        mutations.append(res)

    # transaction

    def commit(self) -> None:
        """Commit this account update transaction.

        May be called multiple times.

        All pending updates are committed with one network request.

        Atomic. The entire transaction either commits or fails with an exception.
        """
        mutations: List[l.SelectionNode] = []

        if len(self._mutations) == 0:
            return

        def _add_mutations_selection(cur):
            if isinstance(cur[0], _AccountRegistryProjectsUpsertData):
                self._add_registry_projects_upsert_selection(cur, mutations)
            if isinstance(cur[0], _AccountRegistryProjectsDeleteData):
                self._add_registry_projects_delete_selection(cur, mutations)

        cur = [self._mutations[0]]
        for mut in self._mutations[1:]:
            if isinstance(mut, type(cur[0])):
                cur.append(mut)
                continue

            _add_mutations_selection(cur)
            cur = [mut]

        _add_mutations_selection(cur)

        sel_set = l.SelectionSetNode()
        sel_set.selections = tuple(mutations)

        doc = l.parse("""
            mutation AccountUpdate {
                placeholder
            }
        """)

        assert len(doc.definitions) == 1
        mut = doc.definitions[0]

        assert isinstance(mut, l.OperationDefinitionNode)
        mut.selection_set = sel_set

        execute(doc)

        self.clear()

    def clear(self):
        """Remove pending updates.

        May be called to cancel any pending updates that have not been committed.
        """
        self._mutations.clear()
