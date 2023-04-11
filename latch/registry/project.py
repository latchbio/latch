from dataclasses import dataclass, field
from typing import List, Literal, Optional, TypedDict, overload

import gql

from latch.gql._execute import execute
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

    def __repr__(self):
        display_name = self.get_display_name(load_if_missing=False)
        if display_name is not None:
            return f"Project(id={self.id}, display_name={display_name})"

        return f"Project(id={self.id})"

    def __str__(self):
        return repr(self)
