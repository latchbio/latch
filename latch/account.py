from typing import List, Optional

from latch.gql.execute import execute
from latch.registry.filter import NumberFilter, StringFilter
from latch.registry.project import Project


class Account:
    def __init__(
        self,
        id: str,
    ):
        self.id = id

    @classmethod
    def current(cls):
        account_id = execute(
            document="""
                query accountInfoQuery {
                    accountInfoCurrent {
                        id
                    }
                }
            """,
        )["accountInfoCurrent"]["id"]

        return cls(id=account_id)

    def list_projects(
        self,
        id: Optional[NumberFilter] = None,
        display_name: Optional[StringFilter] = None,
    ) -> List[Project]:
        filters = []
        if id is not None:
            filters.append(f"id: {id}")
        if display_name is not None:
            filters.append(f"displayName: {display_name}")
        filter_str = "\n".join(filters)

        if len(filter_str) == 0:
            query = f"""
                query ProjectsQuery ($argOwnerId: BigInt!) {{
                    catalogProjects (
                        condition: {{
                            ownerId: $argOwnerId
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
        else:
            query = f"""
                query ProjectsQuery ($argOwnerId: BigInt!) {{
                    catalogProjects (
                        condition: {{
                            ownerId: $argOwnerId
                            removed: false
                        }}
                        filter: {{
                            {filter_str}
                        }}
                    ) {{
                        nodes {{
                            id
                            displayName
                        }}
                    }}
                }}
            """

        data = execute(query, {"argOwnerId": self.id})

        return [Project(node["id"]) for node in data["catalogProjects"]["nodes"]]
