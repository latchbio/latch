"Utilites for registration." ""

import base64
import builtins
import contextlib
import tempfile
from pathlib import Path
from types import ModuleType
from typing import List, Optional

import boto3
import requests
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.tools import module_loader

from latch_cli.services.register import RegisterCtx
from latch_cli.utils import current_workspace


def _login(ctx: RegisterCtx):

    headers = {"Authorization": f"Bearer {ctx.token}"}
    data = {"pkg_name": ctx.image, "ws_account_id": current_workspace()}
    response = requests.post(ctx.latch_image_api_url, headers=headers, json=data)

    try:
        response = response.json()
        access_key = response["tmp_access_key"]
        secret_key = response["tmp_secret_key"]
        session_token = response["tmp_session_token"]
    except KeyError as err:
        raise ValueError(f"malformed response on image upload: {response}") from err

    # TODO: cache
    try:
        client = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name="us-west-2",
        ).client("ecr")
        token = client.get_authorization_token()["authorizationData"][0][
            "authorizationToken"
        ]
    except Exception as err:
        raise ValueError(
            f"unable to retreive an ecr login token for user {ctx.account_id}"
        ) from err

    user, password = base64.b64decode(token).decode("utf-8").split(":")
    ctx.dkr_client.login(
        username=user,
        password=password,
        registry=ctx.dkr_repo,
    )


def build_image(
    ctx: RegisterCtx,
    image_name: str,
    context_path: Path,
    dockerfile: Optional[Path] = None,
) -> List[str]:

    _login(ctx)
    if dockerfile is not None:
        dockerfile = str(dockerfile)
    build_logs = ctx.dkr_client.build(
        path=str(context_path),
        dockerfile=dockerfile,
        buildargs={"tag": f"{ctx.dkr_repo}/{image_name}"},
        tag=f"{ctx.dkr_repo}/{image_name}",
        decode=True,
    )

    return build_logs


def upload_image(ctx: RegisterCtx, image_name: str) -> List[str]:

    return ctx.dkr_client.push(
        repository=f"{ctx.dkr_repo}/{image_name}",
        stream=True,
        decode=True,
    )


def serialize_pkg_in_container(
    ctx: RegisterCtx, image_name: str, serialize_dir: Path
) -> List[str]:

    _serialize_cmd = ["make", "serialize"]
    container = ctx.dkr_client.create_container(
        f"{ctx.dkr_repo}/{image_name}",
        command=_serialize_cmd,
        volumes=[str(serialize_dir)],
        host_config=ctx.dkr_client.create_host_config(
            binds={
                str(serialize_dir): {
                    "bind": "/tmp/output",
                    "mode": "rw",
                },
            }
        ),
    )
    container_id = container.get("Id")
    ctx.dkr_client.start(container_id)
    logs = ctx.dkr_client.logs(container_id, stream=True)

    return [x.decode("utf-8") for x in logs], container_id


def register_serialized_pkg(ctx: RegisterCtx, files: List[Path]) -> dict:

    headers = {"Authorization": f"Bearer {ctx.token}"}

    serialize_files = {
        "version": ctx.version.encode("utf-8"),
        ".latch_ws": current_workspace().encode("utf-8"),
    }
    with contextlib.ExitStack() as stack:
        file_handlers = [stack.enter_context(open(file, "rb")) for file in files]
        for fh in file_handlers:
            serialize_files[fh.name] = fh

        response = requests.post(
            ctx.latch_register_api_url,
            headers=headers,
            files=serialize_files,
        )

    return response.json()


def import_flyte_objects(path: Path, module_name: str = "wf"):

    with module_loader.add_sys_path(str(path)):

        # (kenny) Documenting weird failure modes of importing modules:
        #   1. Calling attribute of FakeModule in some nested import
        #
        #   ```
        #   # This is submodule or nested import of top level import
        #   import foo
        #   def new_func(a=foo.something):
        #       ...
        #   ```
        #
        #   The potentially weird workaround is to silence attribute
        #   errors during import, which I don't see as swallowing problems
        #   associated with the strict task here of retrieving attributes
        #   from tasks, but idk.
        #
        #   2. Calling FakeModule directly in nested import
        #
        #   ```
        #   # This is submodule or nested import of top level import
        #   from foo import bar
        #
        #   a = bar()
        #   ```
        #
        #   This is why we return a callable from our FakeModule

        class FakeModule(ModuleType):
            def __getattr__(self, key):
                return lambda: None

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
            except (ModuleNotFoundError, AttributeError):
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
        module_loader.just_load_modules([module_name])
        builtins.__import__ = real_import

        FlyteContextManager.pop_context()
