"""
register
~~~~~
Registers workflows with the latch platform.
"""

import os
import tarfile
import tempfile
import textwrap
from io import BytesIO
from pathlib import Path
from typing import List

import requests
from latch.services.register import RegisterCtx, RegisterOutput


def register(pkg_root: str) -> RegisterOutput:
    """

    * Constructs a container from a workflow package.
    * Serializes workflow objects from within container
    * Registers serialized objects with latch

    """

    ctx = RegisterCtx(pkg_root)

    build_logs = _build_image(ctx)
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td).resolve()
        serialize_logs = _serialize_pkg(ctx, td_path)
        registration_response = _register_serialized_pkg(ctx, td_path)

    return RegisterOutput(
        build_logs=build_logs,
        serialize_logs=serialize_logs,
        registration_response=registration_response,
    )


def _build_image(ctx: RegisterCtx) -> List[str]:

    # Contruct tarball holding docker build context
    # We want to construct a custom context that only has package files + our
    # dockerfile object injected directly from memory.
    def _build_file_list(root: str):
        files = []
        for dirname, dirnames, fnames in os.walk(root):
            for filename in fnames + dirnames:
                longpath = os.path.join(dirname, filename)
                files.append(longpath.replace(root, "", 1).lstrip("/"))
        return files

    with tempfile.NamedTemporaryFile() as f:
        with tarfile.open(mode="w", fileobj=f) as t:

            for path in _build_file_list(ctx.pkg_root):
                full_path = Path(ctx.pkg_root).resolve().joinpath(path)
                i = t.gettarinfo(full_path, arcname=path)

                if i.isfile():
                    try:
                        if full_path.name == "__init__.py":
                            pkg_name_candidate = full_path.parent.name
                            if ctx.pkg_name is not None:
                                raise ValueError(
                                    "Can only register one"
                                    " workflow package at a time."
                                    " Attempted to register both"
                                    f" {ctx.pkg_name} and"
                                    f" {pkg_name_candidate}"
                                )
                            else:
                                ctx.pkg_name = pkg_name_candidate

                        if full_path.name == "version":
                            with open(full_path, "r") as v:
                                ctx.version = v.read().strip()
                                v.seek(0)
                        with open(full_path, "rb") as fp:
                            t.addfile(i, fp)

                    except OSError:
                        raise OSError(f"Can not read file in context: {full_path}")
                else:
                    # Directories, FIFOs, symlinks don't need to be read.
                    t.addfile(i, None)

            if ctx.version is None:
                raise ValueError(
                    "No version file found in {root}. This file is required"
                    " and should contain the package version in plain text."
                )

            dockerfile = textwrap.dedent(
                f"""
                    FROM {ctx.dkr_repo}/wf-base:wf-base-d2fb-main


                    COPY {ctx.pkg_name} /root/{ctx.pkg_name}
                    WORKDIR /root

                    ARG tag
                    ENV FLYTE_INTERNAL_IMAGE $tag
                    """
            )
            dockerfile = BytesIO(dockerfile.encode("utf-8"))
            dfinfo = tarfile.TarInfo("Dockerfile")
            dfinfo.size = len(dockerfile.getvalue())
            dockerfile.seek(0)
            t.addfile(dfinfo, dockerfile)
            f.seek(0)

            build_logs = ctx.dkr_client.build(
                fileobj=f,
                custom_context=True,
                buildargs={"tag": ctx.image_tagged},
                tag=ctx.image_tagged,
            )

    return [x.decode("utf-8") for x in build_logs]


def _serialize_pkg(ctx: RegisterCtx, serialize_dir: Path) -> List[str]:

    _serialize_cmd = ["make", "serialize"]
    container = ctx.dkr_client.create_container(
        ctx.image_tagged,
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

    return [x.decode("utf-8") for x in logs]


def _register_serialized_pkg(ctx: RegisterCtx, serialize_dir: Path) -> dict:

    files = {"version": ctx.version.encode("utf-8")}
    for dirname, dirnames, fnames in os.walk(serialize_dir):
        for filename in fnames + dirnames:
            file = Path(dirname).resolve().joinpath(filename)
            files[file.name] = open(file, "rb")

    headers = {"Authorization": f"Bearer {ctx.token}"}
    response = requests.post(ctx.latch_register_api_url, headers=headers, files=files)
    return response.text
