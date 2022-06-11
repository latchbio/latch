"""
test.test_register
~~~

    - build a Docker image from a latch package
    - serialize latch package within said image
    - retrieve federated credentials + login to latch's container registry
    - upload image to latch's container registry
    - register serialized package with latch api

"""

import shutil
import tempfile
from pathlib import Path

from latch_cli.services.init import _gen__init__
from latch_cli.services.register.models import RegisterCtx
from latch_cli.services.register.register import (
    _register_serialized_pkg,
    _serialize_pkg,
    _upload_pkg_image,
    build_image,
)

from .fixtures import test_account_jwt

_VERSION_0 = "obama"


def _validate_stream(stream, pkg_name, version):

    lines = []
    for chunk in stream:
        lines.append(chunk)

    last_line = lines[-1]["stream"]

    # https://github.com/docker/docker-py/blob/a48a5a9647761406d66e8271f19fab7fa0c5f582/tests/ssh/api_build_test.py#L570
    # Sufficient for moby's official api, suff. for us...
    assert "Successfully tagged" in last_line
    assert pkg_name in last_line
    assert version in last_line


def _setup_and_build_w_dockerfile(jwt, pkg_name):

    with tempfile.TemporaryDirectory() as tmpdir:

        pkg_dir = Path(tmpdir).joinpath(pkg_name)
        pkg_dir.mkdir()
        pkg_dir.joinpath("wf").mkdir()

        with open(pkg_dir.joinpath("wf/__init__.py"), "w") as f:
            f.write(_gen__init__(pkg_name))

        with open(pkg_dir.joinpath("version"), "w") as f:
            f.write(_VERSION_0)

        dockerfile = Path(pkg_dir).joinpath("Dockerfile")
        with open(dockerfile, "w") as df:
            df.write(
                "\n".join(
                    [
                        "FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:9a7d-main",
                        "COPY wf /root/wf",
                        "ARG tag",
                        "ENV FLYTE_INTERNAL_IMAGE $tag",
                        "RUN python3 -m pip install --upgrade latch",
                        "WORKDIR /root",
                    ]
                )
            )

        ctx = RegisterCtx(pkg_root=pkg_dir, token=jwt)
        stream = build_image(ctx, dockerfile=dockerfile)
        _validate_stream(stream, pkg_name, _VERSION_0)
        return ctx


def test_build_image_w_dockerfile(test_account_jwt):

    _setup_and_build_w_dockerfile(test_account_jwt, "foo")


def test_serialize_pkg(test_account_jwt):
    def _setup_serialize(pkg):
        ctx = _setup_and_build_w_dockerfile(test_account_jwt, pkg)
        with tempfile.TemporaryDirectory() as tmpdir:
            logs = _serialize_pkg(ctx, tmpdir)
            # Log order is shuffled
            assert any(["Successfully serialized" in x for x in logs])
            assert any([pkg in x for x in logs])
        return ctx

    _setup_serialize("foo")


def test_image_upload(test_account_jwt):
    def _setup_upload(pkg):
        ctx = _setup_and_build_w_dockerfile(test_account_jwt, pkg)
        with tempfile.TemporaryDirectory() as tmpdir:
            _serialize_pkg(ctx, tmpdir)
            logs = _upload_pkg_image(ctx)
            assert list(logs)[-1]["aux"]["Size"] > 0

    _setup_upload("foo")


def test_pkg_register(test_account_jwt):
    def _setup_register(pkg):
        ctx = _setup_and_build_w_dockerfile(test_account_jwt, pkg)

        # with tempfile.TemporaryDirectory() as tmpdir:

        import secrets

        token = secrets.token_urlsafe(16)
        tmpdir = Path.home().joinpath(f"{token}/")
        tmpdir.mkdir()

        _serialize_pkg(ctx, tmpdir)
        resp = _register_serialized_pkg(ctx, tmpdir)

        shutil.rmtree(str(tmpdir.resolve()))

        stdout = resp["stdout"]
        assert "Success" in stdout

    _setup_register("foo")
