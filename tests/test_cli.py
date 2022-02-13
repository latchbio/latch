import shutil
import subprocess
from pathlib import Path
from typing import List

from .fixtures import project_name, test_account_jwt


def _run_and_verify(cmd: List[str], does_exist: str):
    output = subprocess.run(cmd, capture_output=True, check=True)
    stdout = output.stdout.decode("utf-8")
    assert does_exist in stdout


def test_init(test_account_jwt, project_name):

    _cmd = ["latch", "init", project_name]
    _run_and_verify(_cmd, f"Created a latch workflow called {project_name}.")


def test_register(test_account_jwt, project_name):

    _cmd = ["latch", "register", project_name]
    _run_and_verify(_cmd, "Successfully registered workflow.")
    shutil.rmtree(str(Path(project_name).resolve()))


def test_cp(test_account_jwt):

    with open("foo.txt", "w") as f:
        f.write("foobar")

    _cmd = ["latch", "cp", "foo.txt", "/foo.txt"]
    _run_and_verify(_cmd, "Successfully copied foo.txt to /foo.txt.")
