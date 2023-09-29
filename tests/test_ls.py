import subprocess

from .fixtures import test_account_jwt


def test_ls(test_account_jwt):
    subprocess.run(["latch", "ls"], check=True)
