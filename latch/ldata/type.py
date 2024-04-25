from enum import Enum


class LatchPathError(RuntimeError):
    def __init__(self, message: str, remote_path: str = None, acc_id: str = None):
        super().__init__(message)
        self.message = message
        self.remote_path = remote_path
        self.acc_id = acc_id

    def __str__(self) -> str:
        return f"{self.remote_path}: {self.message}"


class LDataNodeType(str, Enum):
    account_root = "account_root"
    dir = "dir"
    obj = "obj"
    mount = "mount"
    link = "link"
    mount_gcp = "mount_gcp"
    mount_azure = "mount_azure"
