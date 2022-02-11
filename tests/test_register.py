"""
test.test_register
~~~

    - build a Docker image from a latch package
    - serialize latch package within said image
    - retrieve federated credentials + login to latch's container registry
    - upload image to latch's container registry
    - register serialized package with latch api

"""

import glob
import tempfile
from pathlib import Path

import pytest
from latch.services.init import _gen__init__
from latch.services.register.models import RegisterCtx
from latch.services.register.register import (_build_image,
                                              _register_serialized_pkg,
                                              _serialize_pkg,
                                              _upload_pkg_image)


@pytest.fixture(scope="session")
def test_account_jwt():
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiS2VubnkiLCJmYW1pbHlfbmFtZSI6IldvcmttYW4iLCJuaWNrbmFtZSI6Imtlbm55IiwibmFtZSI6Iktlbm55IFdvcmttYW4iLCJwaWN0dXJlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EtL0FPaDE0R2hCOGNKR2Q1RklXUHVPWmUtYlN2QnZ2WDE5MkFFRC1GV3UwendocGdNOTl5RHEzcXF4LUZOMmJWS1VVYlJPR21tYklNS19yeVh2TF9ERDR3YWdOVFNSTGY3NTJJZV9hejlNZTZMSmEzS1cwWUxiUkpWeXFnT0U5SDgxQkVWbEdvNXZYd1ZJN3NuVkFwWlZVZkJnNldwb0gtTkRETDFGQThQSGE4VVlVRnlkZFJRaFlwTUt1eFYzU25TNUJDc3hlSWxoaHJXNS1rTWRpcmNTWmQ4N3ROejdLaExnZFowcWpwQUY3UEljZ1p1QnhoTmN0R3JZNDE5ZXhsS0YwOUs3cFFMSmpROU5Xc3VETHVEMlR6UkxxekxsMk5fekZrM01Idnc4eUxZbFJPNmpSUV9hdUgyRURadUtRcjZWUmNpZ2NqUk1OUjhkbFptajJabmxGR0M4eG9sU3Jfa1MxUGNCalRqV0t3X3lBVkx3R0w4ZzVyY2VWenNOSldhdGViZEtxX29kZUlUbFpWQWd1SERrM3c3dUJvdV9nTUZMcXo5MTBBUkEweXNyNHJsZEVLbkN1N1BTWmhqV1lwYlB5eGhaQlVRZU84OG90TDJsQ1hDUzE0WlNGaDE0WDRJeFZSVFJ1bUtKQ0hmT19HalV4WjdWRko0cWhFVW9iNVhMbTRpaER4Tms1M3ZmaDdleFNwTkNJd01ZY3AxX3R2X0dKcFlxaVJEc0dMNGpDYk96bVlEMndvVjV6M0g5ZE1QS245Wm5XTW1JZXJQNlY1emIxVXZEelJHeFllek4xbm54bUdoVHF2eUZGaE9kSENOTV9qYTRqZG9KU1EzQlhMbnlQTktqcHRIZmdqRGFSX0ZUSXRGQkVZbXRWb0M0VXlBVS1WbHplRlM1emZxU0h2MU4tRU9WZGRqMm41YWhLemwwQnBCQnA3SzU2M1hST0sxczdyME4xMVBoN3cwaTQzYTQ4SDR4M010WDg4RHBYOWs1Y01adzUxdFBSX1BXdDlGY2JFWkhJQl9zM0E9czk2LWMiLCJsb2NhbGUiOiJlbiIsInVwZGF0ZWRfYXQiOiIyMDIyLTAyLTA4VDIxOjIyOjM2LjIwOVoiLCJlbWFpbCI6Imtlbm55QGxhdGNoLmJpbyIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczovL2xhdGNoYWkudXMuYXV0aDAuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTA1MDg3OTM4NjkzMDUwODkxMDI0IiwiYXVkIjoianpGQk9oSWJmcDRFUFJZWjh3bXg0WXl2TDI3TEZEZUIiLCJpYXQiOjE2NDQ2MTY3MjYsImV4cCI6MTY0NDY1MjcyNn0.E9VzK67CkpSh9Dzv7xKEroVgbSGNxtf8uW2GL8jqqDwiDLnU_KYg28009znrdVFlFGS4RndKJUe9w8q0dMof--vfaNtSbiAODIrHewwl3JZKc2F54Imk8as67KsCF5S7OSdAENfD1gTY-hm2SOyjgGiKXNkFlfe61cj3QXdg3KW_x3Iu8PvWDssjge9d2G4BfAgyjMj1TbeCp9Mqgh0gqjA50iHk3AwAVuI7wNi-FBpIUwPgnNTb4V36yvne-aoqTqWPsgrtw7PgiLAp7CesZibvocuFA-00JI5DYgZaTJ5zHp4lIA2kkxf20sRkhpGFdsgc4VvXRfNaXmTf7AUTug"
    return tmp_token


_VERSION_0 = "0.0.0-dev"
_VERSION_1 = "0.0.1"


def _validate_stream(stream, pkg_name, version):

    lines = []
    for chunk in stream:
        lines.append(chunk)

    last_line = lines[-1]["stream"]

    # https://github.com/docker/docker-py/blob/master/tests/ssh/api_build_test.py#L570
    # Sufficient for moby's official api, suff. for us...
    assert "Successfully tagged" in last_line
    assert pkg_name in last_line
    assert version in last_line


def _setup_and_build_wo_dockerfile(jwt, pkg_name, requirements=None):

    with tempfile.TemporaryDirectory() as tmpdir:

        pkg_dir = Path(tmpdir).joinpath(pkg_name)
        pkg_dir.mkdir()

        with open(pkg_dir.joinpath("__init__.py"), "w") as f:
            f.write(_gen__init__(pkg_name))

        with open(pkg_dir.joinpath("version"), "w") as f:
            f.write(_VERSION_0)

        ctx = RegisterCtx(pkg_root=pkg_dir, token=jwt)
        stream = _build_image(ctx, requirements=requirements)
        _validate_stream(stream, pkg_name, _VERSION_0)
        return ctx


def _setup_and_build_w_dockerfile(jwt, pkg_name):

    with tempfile.TemporaryDirectory() as tmpdir:

        pkg_dir = Path(tmpdir).joinpath(pkg_name)
        pkg_dir.mkdir()

        with open(pkg_dir.joinpath("__init__.py"), "w") as f:
            f.write(_gen__init__(pkg_name))

        with open(pkg_dir.joinpath("version"), "w") as f:
            f.write(_VERSION_0)

        dockerfile = Path(tmpdir).joinpath("Dockerfile")
        with open(dockerfile, "w") as df:
            df.write(
                "\n".join(
                    [
                        "FROM busybox",
                        f"COPY {pkg_name} /src/{pkg_name}",
                        "WORKDIR /src",
                    ]
                )
            )

        ctx = RegisterCtx(pkg_root=pkg_dir, token=jwt)
        stream = _build_image(ctx, dockerfile=dockerfile)
        _validate_stream(stream, pkg_name, _VERSION_0)
        return ctx


def test_build_image_wo_dockerfile(test_account_jwt):

    _setup_and_build_wo_dockerfile(test_account_jwt, "foo")
    _setup_and_build_wo_dockerfile(test_account_jwt, "foo-bar")


def test_build_image_w_requirements(test_account_jwt):

    with tempfile.NamedTemporaryFile("w") as f:
        f.write(
            "\n".join(
                [
                    "click==8.0.3",
                    "Flask==2.0.2",
                    "itsdangerous==2.0.1",
                    "Jinja2==3.0.3",
                    "MarkupSafe==2.0.1",
                    "Werkzeug==2.0.2",
                ]
            )
        )
        f.seek(0)
        f = Path(f.name).resolve()

        _setup_and_build_wo_dockerfile(test_account_jwt, "foo", requirements=f)
        _setup_and_build_wo_dockerfile(test_account_jwt, "foo-bar", requirements=f)


def test_build_image_w_dockerfile(test_account_jwt):

    _setup_and_build_w_dockerfile(test_account_jwt, "foo")
    _setup_and_build_w_dockerfile(test_account_jwt, "foo-bar")


def test_serialize_pkg(test_account_jwt):
    def _setup_serialize(pkg):
        ctx = _setup_and_build_wo_dockerfile(test_account_jwt, pkg)
        with tempfile.TemporaryDirectory() as tmpdir:
            logs = _serialize_pkg(ctx, tmpdir)
            # Log order is shuffled
            assert any(["Successfully serialized" in x for x in logs[-3:]])
            assert pkg in logs[-3]
        return ctx

    _setup_serialize("foo")
    _setup_serialize("foo-bar")


def test_image_upload(test_account_jwt):
    def _setup_upload(pkg):
        ctx = _setup_and_build_wo_dockerfile(test_account_jwt, pkg)
        with tempfile.TemporaryDirectory() as tmpdir:
            _serialize_pkg(ctx, tmpdir)
            logs = _upload_pkg_image(ctx)
            assert list(logs)[-1]["aux"]["Size"] > 0

    _setup_upload("foo")
    _setup_upload("foo-bar")


def test_pkg_register(test_account_jwt):
    def _setup_register(pkg):
        ctx = _setup_and_build_wo_dockerfile(test_account_jwt, pkg)

        # with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path("/Users/Shared/foobar/")
        tmpdir.mkdir()

        _serialize_pkg(ctx, tmpdir)

        tmp_name = f"{str(Path(tmpdir).resolve())}/*"

        with open(Path(tmpdir).resolve().joinpath("foo.txt"), "w") as f:
            f.write("foobar")

        print("actual: ", tmp_name)
        print("list: ", glob.glob(tmp_name))

        resp = _register_serialized_pkg(ctx, tmpdir)
        print("list: ", glob.glob(tmp_name))
        print("resp: ", resp)
        stdout = resp["stdout"]
        assert "Success" in stdout
        assert pkg in stdout

    _setup_register("foo")
    _setup_register("foo-bar")
