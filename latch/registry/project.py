import latch.gql as gql
from latch import Account


class Project:
    def __init__(self, project_id: str):
        self.id = project_id

        self._name = None

    @property
    def name(self):
        if self._name is not None:
            return self._name

        self._name = gql.execute(
            document="""
                query projectNameQuery ($argProjectId: BigInt!) {
                    catalogProject(id: $argProjectId) {
                        displayName
                    }
                }
            """,
            variables={"argProjectId": self.id},
        )["catalogProject"]["displayName"]

        return self._name

    @classmethod
    def from_name(cls, account: Account, project_name: str):
        info = gql.execute(
            document="""
                query projectInfoQuery ($argWorkspaceId: BigInt!) {
                    catalogProjects(condition: {
                        ownerId: $argWorkspaceId,
                        removed: false
                    }) {
                        nodes {
                            id
                            displayName
                        }
                    }
                }
            """,
            variables={"argWorkspaceId": account.workspace_id},
        )["catalogProjects"]["nodes"]

        for project in info:
            if project["displayName"] == project_name:
                return cls(project_id=project["id"])

        raise ValueError(
            f"No project named {project_name} found in account {account.workspace_id}."
            " Make sure you are in the correct workspace."
        )
