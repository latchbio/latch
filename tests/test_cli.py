import shutil
import subprocess
import textwrap
from pathlib import Path
from typing import List

from .fixtures import project_name, test_account_jwt


def _run_and_verify(cmd: List[str], does_exist: str):
    output = subprocess.run(cmd, capture_output=True, check=True)
    stdout = output.stdout.decode("utf-8")
    assert does_exist in stdout


def test_init_and_register(test_account_jwt, project_name):
    # Originally two separate tests: test_init and test_register.

    # Combined into one test because pytest randomizes the order of tests, meaning
    # half the time test_register would fail because the project had not been created
    # by test_init yet.

    _cmd = ["latch", "init", project_name]
    _run_and_verify(_cmd, f"Created a latch workflow called {project_name}.")

    _cmd = ["latch", "register", project_name]
    _run_and_verify(_cmd, "Successfully registered workflow.")
    shutil.rmtree(str(Path(project_name).resolve()))


def test_cp(test_account_jwt):
    with open("foo.txt", "w") as f:
        f.write("foobar")

    _cmd = ["latch", "cp", "foo.txt", "latch:///foo.txt"]
    _run_and_verify(_cmd, "Successfully copied foo.txt to latch:///foo.txt.")
    _cmd = ["latch", "cp", "foo.txt", "latch:///fake_dir_that_doesnt_exist/foo.txt"]
    # doesn't do what we expected
    _run_and_verify(
        _cmd,
        "Successfully copied foo.txt to latch:///fake_dir_that_doesnt_exist/foo.txt.",
    )
    _cmd = ["latch", "cp", "foo.txt", "latch:///oof.txt"]
    _run_and_verify(_cmd, "Successfully copied foo.txt to latch:///oof.txt.")
    _cmd = ["latch", "cp", "foo.txt", "latch:///welcome/"]
    _run_and_verify(_cmd, "Successfully copied foo.txt to latch:///welcome/")

    _cmd = ["latch", "cp", "latch:///foo.txt", "bar.txt"]
    _run_and_verify(_cmd, "Successfully copied latch:///foo.txt to bar.txt.")
    _cmd = ["latch", "cp", "latch:///foo.txt", "stuff.txt"]
    _run_and_verify(_cmd, "Successfully copied latch:///foo.txt to stuff.txt.")


def test_ls(test_account_jwt):

    # todo(ayush) add more ls tests
    _cmd = ["latch", "ls"]
    _run_and_verify(_cmd, "welcome")


def test_execute(test_account_jwt):

    with open("foo.py", "w") as f:
        f.write(
            textwrap.dedent(
                """
            from latch.types import LatchFile

            params = {
                "_name": "wf.assemble_and_sort",
                "read1": LatchFile("latch:///read1"),
                "read2": LatchFile("latch:///read2"),
            }
        """
            )
        )

    _cmd = ["latch", "execute", "foo.py"]
    _run_and_verify(
        _cmd,
        "Successfully launched workflow named wf.assemble_and_sort with version latest.",
    )


def test_get_wf(test_account_jwt):

    _cmd = ["latch", "get-wf"]
    _run_and_verify(_cmd, "latch.crispresso2_wf")

    _cmd = ["latch", "get-wf", "--name", "latch.crispresso2_wf"]
    _run_and_verify(_cmd, "v0.1.11")
