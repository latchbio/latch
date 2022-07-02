import os
import secrets
import shutil
import string
import subprocess
import textwrap
from pathlib import Path
from typing import List

import pytest
import requests

from latch_cli.config.latch import LatchConfig

from .fixtures import project_name, test_account_jwt

config = LatchConfig()
endpoints = config.sdk_endpoints


def _random_name(length: int):
    return "".join(secrets.choice(string.ascii_letters) for _ in range(length))


def _normalize_remote_path(path: str):
    if path.startswith("latch://"):
        path = path[len("latch://") :]
    path = path.strip("/")
    return path


def _run_and_verify(cmd: List[str], does_exist: str):
    output = subprocess.run(cmd, capture_output=True, check=True)
    stdout = output.stdout.decode("utf-8")
    assert does_exist in stdout


def _file_exists(token, remote_dir: str, filename: str) -> bool:
    filename = _normalize_remote_path(filename)
    remote_dir = _normalize_remote_path(remote_dir)

    if not remote_dir:
        remote_path = filename
    else:
        remote_path = f"{remote_dir}/{filename}"

    headers = {"Authorization": f"Bearer {token}"}
    data = {"filename": remote_path}
    response = requests.post(url=endpoints["verify"], headers=headers, json=data)
    try:
        assert response.status_code == 200
    except:
        raise ValueError(f"{response.content}")
    return response.json()["exists"]


def _run_mkdir_touch_recursive(token, curr_dir: str, branching_factor: int, depth: int):
    if depth > 2:
        return
    curr_dir = _normalize_remote_path(curr_dir)
    for _ in range(branching_factor):
        name = _random_name(10)
        if not curr_dir:
            remote_path = name
        else:
            remote_path = f"{curr_dir}/{name}"
        operation = secrets.choice(["mkdir", "touch"])
        _cmd = ["latch", operation, remote_path]
        _run_and_verify(_cmd, "Success")
        assert _file_exists(token, curr_dir, name)
        if operation == "mkdir":
            _run_mkdir_touch_recursive(token, remote_path, branching_factor, depth + 1)
        _cmd = ["latch", "rm", remote_path]
        _run_and_verify(_cmd, "Success")


def _run_nested_cp(token, curr_dir: str, filename: str, depth: int):
    if depth > 5:
        return
    filename = _normalize_remote_path(filename)
    curr_dir = _normalize_remote_path(curr_dir)
    _cmd = ["latch", "mkdir", curr_dir]
    _run_and_verify(_cmd, f"Successfully created directory {curr_dir}.")
    _run_cp_and_clean_up(token, curr_dir, filename)
    nested_dir_name = _random_name(10)
    nested_filename = _random_name(10)
    _run_nested_cp(token, f"{curr_dir}/{nested_dir_name}", nested_filename, depth + 1)
    _cmd = ["latch", "rm", curr_dir]
    _run_and_verify(_cmd, f"Successfully deleted {curr_dir}.")


def _run_cp_and_clean_up(token, remote_dir: str, filename: str):
    """
    Checks that
        (1) the file was actually copied to latch, and
        (2) the file contents do not change from local -> latch -> local
    """
    filename = _normalize_remote_path(filename)
    remote_dir = _normalize_remote_path(remote_dir)

    initial = Path(f"initial_{filename}").resolve()
    final = Path(f"final_{filename}").resolve()
    try:
        if not remote_dir:
            remote_path = f"latch:///{filename}"
        else:
            remote_path = f"latch:///{remote_dir}/{filename}"
        initial_text = _random_name(100)
        with open(initial, "w") as f:
            f.write(initial_text)
        cmd = ["latch", "cp", initial, remote_path]
        _run_and_verify(cmd, f"Successfully copied {initial} to {remote_path}")
        assert _file_exists(token, remote_dir, filename)
        cmd = ["latch", "cp", remote_path, final]
        _run_and_verify(cmd, f"Successfully copied {remote_path} to {final}")
        with open(final, "r") as f:
            final_text = f.read()
        assert initial_text == final_text
        cmd = ["latch", "rm", remote_path]
        _run_and_verify(cmd, f"Successfully deleted {remote_path}")
        assert not _file_exists(token, remote_dir, filename)
    finally:
        if os.path.isfile(initial):
            os.remove(initial)
        if os.path.isfile(final):
            os.remove(final)


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


def test_cp_home_robustness(test_account_jwt):
    for _ in range(5):
        filename = _random_name(10)
        filename = f"{filename}.txt"
        _run_cp_and_clean_up(test_account_jwt, "", filename)


def test_cp_nested(test_account_jwt):
    initial_dir_name = _random_name(10)
    initial_filename = _random_name(10)
    _run_nested_cp(test_account_jwt, initial_dir_name, initial_filename, 0)


def test_touch_mkdir_higher_branching_factor(test_account_jwt):
    # don't do any more than 3 for the branching_factor
    _run_mkdir_touch_recursive(test_account_jwt, "/", branching_factor=3, depth=2)


@pytest.mark.xfail(strict=True)
def test_bad_input_cp_1():
    name1 = _random_name(10)
    name2 = _random_name(10)
    _cmd = ["latch", "cp", name1, name2]
    _run_and_verify(_cmd, "Success")


@pytest.mark.xfail(strict=True)
def test_bad_input_cp_2():
    name1 = _random_name(10)
    name2 = _random_name(10)
    _cmd = ["latch", "cp", f"latch:///{name1}", f"latch:///{name2}"]
    _run_and_verify(_cmd, "Success")


def test_ls(test_account_jwt):
    for _ in range(5):
        name = _random_name(10)
        _cmd = ["latch", "mkdir", name]
        _run_and_verify(_cmd, "Success")
        _cmd = ["latch", "ls"]
        _run_and_verify(_cmd, name)
        _cmd = ["latch", "rm", name]
        _run_and_verify(_cmd, "Success")


def test_launch(test_account_jwt):

    with open("foo.py", "w") as f:
        f.write(
            textwrap.dedent(
                """
            from latch.types import LatchFile

            params = {
                "_name": "wf.__init__.assemble_and_sort",
                "read1": LatchFile("latch:///read1"),
                "read2": LatchFile("latch:///read2"),
            }
        """
            )
        )

    _cmd = ["latch", "launch", "foo.py"]
    _run_and_verify(
        _cmd,
        "Successfully launched workflow named wf.__init__.assemble_and_sort with"
        " version latest.",
    )


def test_get_wf(test_account_jwt):

    _cmd = ["latch", "get-wf"]
    _run_and_verify(_cmd, "wf.__init__.crispresso2_wf")

    _cmd = ["latch", "get-wf", "--name", "wf.__init__.crispresso2_wf"]
    _run_and_verify(_cmd, "v0.1.11")
