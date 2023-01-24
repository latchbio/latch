import builtins
import contextlib
import os
import random
import string
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Iterator, List, Optional

import docker
import paramiko
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.tools import module_loader


@contextlib.contextmanager
def _add_sys_paths(paths: List[Path]) -> Iterator[None]:
    paths = [os.fspath(p) for p in paths]
    try:
        for p in paths:
            sys.path.insert(0, p)
        yield
    finally:
        for p in paths:
            sys.path.remove(p)


def _import_flyte_objects(paths: List[Path], module_name: str = "wf"):

    with _add_sys_paths(paths):

        class FakeModule(ModuleType):
            def __getattr__(self, key):
                # This value is designed to be used in the execution of top
                # level code without throwing errors but need not do anything.
                # Here are some cases where this value can be used:
                #
                # ```
                # from foo import bar
                #
                # # Referencing an attribute from an attribute
                # a = bar.x
                #
                # # Calling an attribute
                # b = bar()
                #
                # # Subclassing from an attribute
                # class C(bar):
                # ```
                #     ...
                class FakeAttr:
                    def __new__(*args, **kwargs):
                        return None

                return FakeAttr

            __all__ = []

        real_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            try:
                return real_import(
                    name,
                    globals=globals,
                    locals=locals,
                    fromlist=fromlist,
                    level=level,
                )
            except (ModuleNotFoundError, AttributeError) as e:
                return FakeModule(name)

        # Temporary ctx tells lytekit to skip local execution when
        # inspecting objects
        fap = FileAccessProvider(
            local_sandbox_dir=tempfile.mkdtemp(prefix="foo"),
            raw_output_prefix="bar",
        )
        tmp_context = FlyteContext(fap, inspect_objects_only=True)

        FlyteContextManager.push_context(tmp_context)
        builtins.__import__ = fake_import

        mods = list(module_loader.iterate_modules([module_name]))

        builtins.__import__ = real_import
        FlyteContextManager.pop_context()

        return mods


def _construct_dkr_client(ssh_host: Optional[str] = None):
    """Try many methods of establishing valid connection with client.

    This was helpful -
    https://github.com/docker/docker-py/blob/a48a5a9647761406d66e8271f19fab7fa0c5f582/docker/utils/utils.py#L321

    If `ssh_host` is passed, we attempt to make a connection with a remote
    machine.
    """

    def _from_env():

        host = environment.get("DOCKER_HOST")

        # empty string for cert path is the same as unset.
        cert_path = environment.get("DOCKER_CERT_PATH")
        if cert_path == "":
            cert_path = None

        # empty string for tls verify counts as "false".
        # Any value or 'unset' counts as true.
        tls_verify = environment.get("DOCKER_TLS_VERIFY") != ""

        enable_tls = tls_verify or cert_path is not None

        dkr_client = None
        try:
            if not enable_tls:
                dkr_client = docker.APIClient(host)
            else:
                if not cert_path:
                    cert_path = os.path.join(os.path.expanduser("~"), ".docker")

                tls_config = docker.tls.TLSConfig(
                    client_cert=(
                        os.path.join(cert_path, "cert.pem"),
                        os.path.join(cert_path, "key.pem"),
                    ),
                    ca_cert=os.path.join(cert_path, "ca.pem"),
                    verify=tls_verify,
                )
                dkr_client = docker.APIClient(host, tls=tls_config)

        except docker.errors.DockerException as de:
            raise OSError(
                "Unable to establish a connection to Docker. Make sure that"
                " Docker is running and properly configured before attempting"
                " to register a workflow."
            ) from de

        return dkr_client

    if ssh_host is not None:
        try:
            return docker.APIClient(ssh_host, use_ssh_client=True)
        except docker.errors.DockerException as de:
            raise OSError(
                f"Unable to establish a connection to remote docker host {ssh_host}."
            ) from de

    environment = os.environ
    host = environment.get("DOCKER_HOST")
    if host is not None and host != "":
        return _from_env()
    else:
        try:
            # TODO: platform specific socket defaults
            return docker.APIClient(base_url="unix://var/run/docker.sock")
        except docker.errors.DockerException as de:
            raise OSError(
                "Docker is not running. Make sure that"
                " Docker is running before attempting to register a workflow."
            ) from de


def _construct_ssh_client(host_ip: str, username: str):

    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(host_ip, username=username)
    return ssh


class _TmpDir:

    """Represents a temporary directory that can be local or on a remote machine."""

    def __init__(self, ssh_client=None, remote=False):

        if remote and not ssh_client:
            raise ValueError("Must provide an ssh client if remote is True.")

        self.remote = remote
        self.ssh_client = ssh_client
        self._tempdir = None

    def __enter__(self, *args):
        return self.create(*args)

    def __exit__(self, *args):
        self.cleanup(*args)

    def create(self, *args):
        if not self.remote:
            self._tempdir = tempfile.TemporaryDirectory()
            return Path(self._tempdir.name).resolve()
        else:
            td = "".join(
                random.choices(
                    string.ascii_uppercase + string.ascii_lowercase + string.digits, k=8
                )
            )
            self._tempdir = f"/tmp/{td}"
            self.ssh_client.exec_command(f"mkdir {self._tempdir}")
            return self._tempdir

    def cleanup(self, *args):
        if not self.remote:
            self._tempdir.cleanup()
        else:
            self.ssh_client.exec_command(f"rm -rf {self._tempdir}")
