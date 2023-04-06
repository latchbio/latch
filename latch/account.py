from dataclasses import dataclass
from typing import List, Optional

from latch.gql.execute import execute
from latch.registry.project import Project


@dataclass(frozen=True)
class Account:
    id: str

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

    def list_projects(self) -> List[Project]:
        query = """
            query ProjectsQuery ($argOwnerId: BigInt!) {
                catalogProjects (
                    condition: {
                        ownerId: $argOwnerId
                        removed: false
                    }
                ) {
                    nodes {
                        id
                        displayName
                    }
                }
            }
        """

        data = execute(query, {"argOwnerId": self.id})

        return [Project(node["id"]) for node in data["catalogProjects"]["nodes"]]
