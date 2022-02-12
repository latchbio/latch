import subprocess
from typing import List


def _run_and_verify(cmd: List[str], does_exist: str):
    output = subprocess.run(cmd, capture_output=True, check=True)
    stdout = output.stdout.decode("utf-8")
    assert does_exist in stdout


def test_init():

    _cmd = ["latch", "init", "foobar"]
    _run_and_verify(_cmd, "Created a latch workflow called foobar.")


def test_register():

    _cmd = ["latch", "register", "foobar"]
    _run_and_verify(_cmd, "Successfully registered workflow.")


def test_cp():

    with open("foo.txt", "w") as f:
        f.write("foobar")

    _cmd = ["latch", "cp", "foo.txt", "/foo.txt"]
    _run_and_verify(_cmd, "Successfully copied foo.txt to /foo.txt.")
