import secrets
import shutil
import string
import subprocess
import textwrap
from pathlib import Path
from typing import List

import requests

from latch.config import ENV, LatchConfig

from .fixtures import project_name, test_account_jwt

config = LatchConfig(ENV)
endpoints = config.sdk_endpoints


def _run_and_verify(cmd: List[str], does_exist: str):
    output = subprocess.run(cmd, capture_output=True, check=True)
    stdout = output.stdout.decode("utf-8")
    assert does_exist in stdout


def _file_exists(token, filename: str) -> bool:
    if not filename[0] == "/":
        filename = f"/{filename}"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"filename": filename}
    response = requests.post(url=endpoints["verify"], headers=headers, json=data)
    try:
        assert response.status_code == 200
    except:
        raise ValueError(f"{response.content}")
    return response.json()["exists"]


def _remove_file(token, filename: str):
    if not filename[0] == "/":
        filename = f"/{filename}"
    data = {"filename": filename}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url=endpoints["remove"], headers=headers, json=data)
    try:
        assert response.status_code == 200
    except:
        raise ValueError(f"{response.content}")


def _run_cp_and_clean_up(token, filename: str):
    """
    Checks that
        (1) the file was actually copied to latch, and
        (2) the file contents do not change from local -> latch -> local
    """
    initial = Path(f"initial_{filename}").resolve()
    final = Path(f"final_{filename}").resolve()
    try:
        initial_text = "".join(secrets.choice(string.ascii_letters) for _ in range(100))
        with open(initial, "w") as f:
            f.write(initial_text)
        cmd = ["latch", "cp", initial, f"latch:///{filename}"]
        _run_and_verify(cmd, f"Successfully copied {initial} to latch:///{filename}")
        assert _file_exists(token, filename)
        cmd = ["latch", "cp", f"latch:///{filename}", final]
        _run_and_verify(cmd, f"Successfully copied latch:///{filename} to {final}")
        with open(final, "r") as f:
            final_text = f.read()
        assert initial_text == final_text
        _remove_file(token, filename)
    finally:
        try:
            initial.unlink(missing_ok=True)
            final.unlink(missing_ok=True)
        except TypeError:
            initial.unlink()
            final.unlink()


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
    for _ in range(10):
        filename = "".join(secrets.choice(string.ascii_letters) for _ in range(10))
        filename = f"{filename}.txt"
        _run_cp_and_clean_up(test_account_jwt, filename)


def test_ls(test_account_jwt):

    # TODO(ayush) add more ls tests
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
