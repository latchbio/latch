from dataclasses import dataclass
from typing import Optional

from latch.gql.execute import execute
from latch.registry.table import Table


@dataclass
class Project:
    id: str

    def __post_init__(self):
        self._display_name: Optional[str] = None

    def get_display_name(self):
        if self._display_name is not None:
            return self._display_name

        self._display_name = execute(
            document="""
                query ProjectQuery ($argProjectId: BigInt!) {
                    catalogProject(id: $argProjectId) {
                        id
                        displayName
                    }
                }
            """,
            variables={"argProjectId": self.id},
        )["catalogProject"]["displayName"]

        return self._display_name

    def list_tables(self):
        query = f"""
            query ExperimentsQuery ($argProjectId: BigInt!) {{
                catalogExperiments (
                    condition: {{
                        projectId: $argProjectId
                        removed: false
                    }}
                ) {{
                    nodes {{
                        id
                        displayName
                    }}
                }}
            }}
        """

        data = execute(query, {"argProjectId": self.id})

        return [Table(node["id"]) for node in data["catalogExperiments"]["nodes"]]

    def __repr__(self):
        return f"Project(id={self.id})"

    def __str__(self):
        if self._display_name is not None:
            return f"Project(display_name={self._display_name})"
        return self.__repr__()
