"""
test.test_register
~~~

    - build a Docker image from a latch package
    - serialize latch package within said image
    - retrieve federated credentials + login to latch's container registry
    - upload image to latch's container registry
    - register serialized package with latch api

"""

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
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiS2VubnkiLCJmYW1pbHlfbmFtZSI6IldvcmttYW4iLCJuaWNrbmFtZSI6Imtlbm55IiwibmFtZSI6Iktlbm55IFdvcmttYW4iLCJwaWN0dXJlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EtL0FPaDE0R2ppMVg3ZmFlaEhlemJ3MHFnQ1ZXSDUtLWpyR2tBbFpDWWxaX0sxTkRTa0hQZGFiVzhaa3UyVm9qZm1PWlNCZnB5VEQ3bngxci14cTJ1VmZhdE9Lbm05VFV3Q1FGNnU1d05kNUktMzAzZEE3aTdydUlBa3Y2dFB4VlN6YmhyTmRrYUNMZGNpN3o0aGJad0h2Nk96QVJZa1NZY2pMYkUtclFsUGpKOE9HZmpEWGgwYXl3cW1DY0RaR1NEdWw2bmdpeE5CVjd2T21ldE5VTU40bkp5NmNMSWoybF9oemVVM2k1eS15a1JUNXZlYjNrVzBQYnFMWDB1TVl2OVFhZW5YQ1dZT2MtMG9aMXMtWUxHY2UxQVREOTBTWTdUU1U1N05jZzhCVWZnczM2VzBpSm1od2FjaS03ZzlaSVVjMlFjamhxMWtBTXBNVldwUFRfSmVsejU2WHh3OF9kWFNLdGxaZ2J5eGMteENyOHBUYXJtYnlnSGFDOS1uQ2lLb1I1T2pSR2czamVTcEZxclRGV1E2NFB6NndLWkN4cTMtbjlSYnJmSU9nQjVwRjVsR3h2Q2RWNVd1MEtadm9pN1FfOFJPaUZRQ2Z6S0VMZFlJZjBCRDZNa185cEpNZ3dHNHpDcnVObmRPWUNmdDJkaWFxc3RPS2pHLXdFaEJ6TWZYZDJ5M1B1a1daSHFUd1IwQVJqN3JHNTY0dHY1cnNQeVNXMGJWZHV5OHYxQWdNR01JZ2V0UW9yeVAxNVRFSkdDM29HTm5yUHdobTJyeVhmNWNwaHdhTzFFVUMxSmhkblNmTWlGWmxhM2FRTHhDSnlHRDA0Z0JnNXlmcUJobkpLczRkQmxFX200V2JhSXF3V1lsNFZvZlNBcUFhel85T0tPRkpubnRVOFI2dnJJUGNfUFVRSFhTa3JEQWNpaFZqOF9xaTM0WVcxTTh4TmdKM2diZFZPSGlLRk1rRy1oOGo2SXVCaWNiUUFoTm1vbDR2Um1LdWRhYmFwQ3pac0ZVRWJLcEpYVUljMXFDSmc9czk2LWMiLCJsb2NhbGUiOiJlbiIsInVwZGF0ZWRfYXQiOiIyMDIyLTAyLTAxVDIzOjUwOjI2LjU0NFoiLCJlbWFpbCI6Imtlbm55QGxhdGNoLmJpbyIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczovL2xhdGNoYWkudXMuYXV0aDAuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTA1MDg3OTM4NjkzMDUwODkxMDI0IiwiYXVkIjoianpGQk9oSWJmcDRFUFJZWjh3bXg0WXl2TDI3TEZEZUIiLCJpYXQiOjE2NDM5MjUxNTgsImV4cCI6MTY0Mzk2MTE1OH0.moqmwh0OgbxK1PDzTuCOE_uR_aajUtUIyejkZiC4QFvIy5FpJvvNUPU9QUz--M9-48T_NPfRZFWjdu_kikNG8YB2_cJc6VKJJeK4GlhonspGSOj4-ubRjPBI-JLNwiHYWofBu9M7L3_ZZUe0mR0pdtp-3SJyhaaNILsNgRK57eQORj-tkNIHvIrHtG3UjulzAg1XwSGWoJeOUAqbs5xVsNS4EyvYLB-RZ6JBuehGwXM-71-6uhZ-b2MLLpQaFBS0p9a2eg4LC6MNlE3RBop1h_09_HQ_67j9r9-LlHoGyHIbkoHQXWC7giAnsYMGtNLJTlnxG4ni15my_cwFLnkBwQ"
    return tmp_token


_VERSION_0 = "0.0.0"
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
        with tempfile.TemporaryDirectory() as tmpdir:
            _serialize_pkg(ctx, tmpdir)
            resp = _register_serialized_pkg(ctx, tmpdir)
            stdout = resp["stdout"]
            assert "Success" in stdout
            assert pkg in stdout

    _setup_register("foo")
    _setup_register("foo-bar")
