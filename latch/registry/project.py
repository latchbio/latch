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
    """Internal cache class to organize information for a `Project`."""

    display_name: Optional[str] = None
    tables: Optional[List[Table]] = None


@dataclass(frozen=True)
class Project:
    """A python representation of a Registry Project.

    This class mirrors a Project in Registry, and provides functionality for
    programmatically getting information about the Project, namely its display
    name and the Tables it contains.

    `Project`s can be instantiated either by calling `Account.list_projects()`
    or directly by ID. A `Project` exposes the getters `get_display_name` and
    `get_tables`. These are documented further in their respective docstrings.

    Attributes:
        id:
            The ID of the underlying Project as a string,
        _cache:
            A private cache for values that need to be queried over the network,
            should not be interacted with directly.
    """

    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    id: str

    def load(self) -> None:
        """Loads all properties at once.

        Performs a GraphQL request and uses the results to populate the
        `display_name` and `tables` properties of the calling Project's cache.
        This is called by `.get_display_name()` and `.list_tables()` when
        `load_if_missing` is set to True (the default).
        """

        data = execute(
            document=gql.gql(
                """
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
                """
            ),
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
        """Gets the display name of the Project, loading it if necessary.

        This function will return the calling Project's display name. If
        `.load()` has not been called yet, and if `load_if_missing` is set to
        True, a call to `.load()` will be made to populate everything.

        Args:
            load_if_missing:
                Keyword-only. Controls whether or not a call to `.load()` will
                be made if the value has not already been queried.
                True by default.

        Returns:
            The display name of the calling Project as a string. Returns None if
            the display name has not been queried yet and `load_if_missing` is
            set to False.
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
        """Gets a list of `Table` objects for each table in this Project.

        This function will return a list of `Table` objects, one for each Table
        in the calling Project. If `.load()` has not been called yet, and if
        `load_if_missing` is set to True, a call to `.load()` will be made to
        populate everything.

        Args:
            load_if_missing:
                Keyword-only. Controls whether or not a call to `.load()` will
                be made if the value has not already been queried.
                True by default.

        Returns:
            A list of `Table`s, each corresponding to a Table within the calling
            Project. Returns None if `.load()` has not been called yet and
            `load_if_missing` is set to False.
        """

        if self._cache.tables is None and load_if_missing:
            self.load()

        return self._cache.tables

    def __str__(self) -> str:
        name = self.get_display_name(load_if_missing=False)
        if name is not None:
            return f"Project({repr(name)})"

        return repr(self)
