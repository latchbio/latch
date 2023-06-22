from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, List, Literal, Optional, TypedDict, Union, overload

import gql
import graphql.language as l
from latch_sdk_gql.execute import execute
from latch_sdk_gql.utils import _GqlJsonValue, _json_value, _name_node, _parse_selection
from typing_extensions import TypeAlias

from latch.registry.table import Table


class _CatalogExperimentNode(TypedDict):
    id: str
    displayName: str


@dataclass
class _Cache:
    display_name: Optional[str] = None
    tables: Optional[List[Table]] = None


@dataclass(frozen=True)
class Project:
    """Registry project (folder containing :class:`tables <latch.registry.table.Table>`).

    :meth:`~latch.account.Account.list_registry_projects` is the typical way to get a :class:`Project`.
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

    def load(self) -> None:
        """(Re-)populate this project instance's cache.

        Future calls to most getters will return immediately without making a network request.

        Always makes a network request.
        """
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

        self._cache.tables = []
        experiments: List[_CatalogExperimentNode] = data[
            "catalogExperimentsByProjectId"
        ]["nodes"]
        for x in experiments:
            cur = Table(x["id"])
            cur._cache.display_name = x["displayName"]
            self._cache.tables.append(cur)

    # get_display_name

    @overload
    def get_display_name(self, *, load_if_missing: Literal[True] = True) -> str:
        ...

    @overload
    def get_display_name(self, *, load_if_missing: bool) -> Optional[str]:
        ...

    def get_display_name(self, *, load_if_missing: bool = True) -> Optional[str]:
        """Get the display name of this project.

        This is an opaque string that can contain any valid Unicode data.

        Display names are *not unique* and *must never be used as identifiers*.
        Use :attr:`id` instead.

        Args:
            load_if_missing:
                If true, :meth:`load` the display name if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Display name.
        """
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
        """List Registry tables contained in this project.

        Args:
            load_if_missing:
                If true, :meth:`load` the table list if not in cache.
                If false, return `None` if not in cache.

        Returns:
            Tables in this project.
        """
        if self._cache.tables is None and load_if_missing:
            self.load()

        return self._cache.tables

    @contextmanager
    def update(self, *, reload_on_commit: bool = True) -> Iterator["ProjectUpdate"]:
        """Start an update transaction.

        The transaction will commit when the context manager closes unless an error occurs.

        No changes will occur until the transaction commits.

        The transaction can be cancelled by running :meth:`ProjectUpdate.clear`
        before closing the context manager.

        Args:
            reload_on_commit:
                If true, :meth:`load` this project after the transaction commits.

        Returns:
            Context manager for the new transaction.
        """

        upd = ProjectUpdate(self)
        yield upd
        upd.commit()

        if reload_on_commit:
            self.load()

    def __repr__(self):
        display_name = self.get_display_name(load_if_missing=False)
        if display_name is not None:
            return f"Project(id={self.id}, display_name={display_name})"

        return f"Project(id={self.id})"

    def __str__(self):
        return repr(self)


@dataclass(frozen=True)
class _ProjectTablesUpsertData:
    display_name: str


@dataclass(frozen=True)
class _ProjectTablesDeleteData:
    id: str


_ProjectTablesMutationData: TypeAlias = Union[
    _ProjectTablesUpsertData, _ProjectTablesDeleteData
]


@dataclass(frozen=True)
class ProjectUpdate:
    _table_mutations: List[_ProjectTablesMutationData] = field(
        default_factory=list,
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    project: Project

    # upsert table

    def upsert_table(self, display_name: str):
        """Creates a table.

        Not idempotent. Two calls with the same args will create two tables.

        Args:
            display_name: The display name of the new table.
        """
        self._table_mutations.append(_ProjectTablesUpsertData(display_name))

    def _add_table_upserts_selection(
        self, upserts: List[_ProjectTablesUpsertData], mutations: List[l.SelectionNode]
    ):
        if len(upserts) == 0:
            return

        display_names: _GqlJsonValue = [x.display_name for x in upserts]

        res = _parse_selection("""
            catalogMultiCreateExperiments(input: {}) {
                clientMutationId
            }
            """)
        assert isinstance(res, l.FieldNode)

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value(
            {
                "argProjectId": self.project.id,
                "argDisplayNames": display_names,
            }
        )

        res.alias = _name_node(f"upd{len(mutations)}")
        res.arguments = tuple([args])

        mutations.append(res)

    # delete table

    def delete_table(self, id: str):
        """Deletes a table.

        Args:
            id: The ID of the target table.
        """
        self._table_mutations.append(_ProjectTablesDeleteData(id))

    def _add_table_deletes_selection(
        self, deletes: List[_ProjectTablesDeleteData], mutations: List[l.SelectionNode]
    ):
        if len(deletes) == 0:
            return

        ids: _GqlJsonValue = [x.id for x in deletes]

        res = _parse_selection("""
            catalogMultiDeleteExperiments(input: {}) {
                clientMutationId
            }
            """)
        assert isinstance(res, l.FieldNode)

        args = l.ArgumentNode()
        args.name = _name_node("input")
        args.value = _json_value(
            {
                "argIds": ids,
            }
        )

        res.alias = _name_node(f"upd{len(mutations)}")
        res.arguments = tuple([args])

        mutations.append(res)

    # transaction

    def commit(self) -> None:
        """Commit this project update transaction.

        May be called multiple times.

        All pending updates are committed with one network request.

        Atomic. The entire transaction either commits or fails with an exception.
        """
        mutations: List[l.SelectionNode] = []

        if len(self._table_mutations) == 0:
            return

        def _add_table_mutations_selection(cur):
            if isinstance(cur[0], _ProjectTablesUpsertData):
                self._add_table_upserts_selection(cur, mutations)
            if isinstance(cur[0], _ProjectTablesDeleteData):
                self._add_table_deletes_selection(cur, mutations)

        cur = [self._table_mutations[0]]
        for mut in self._table_mutations[1:]:
            if isinstance(mut, type(cur[0])):
                cur.append(mut)
                continue

            _add_table_mutations_selection(cur)
            cur = [mut]

        _add_table_mutations_selection(cur)

        sel_set = l.SelectionSetNode()
        sel_set.selections = tuple(mutations)

        doc = l.parse("""
            mutation ProjectUpdate {
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
        self._table_mutations.clear()
