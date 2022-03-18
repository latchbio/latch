"""
Using flytekit internals to serialize instead of pyflyte
"""

import os

import flytekit
from flytekit.clis.sdk_in_container.serialize import SerializationMode, serialize_all

#
# SERIALIZE

# usually in flytekit.config
pkgs = ["foo"]

# ctx.obj[CTX_LOCAL_SRC_ROOT]
dir = local_source_root = os.getcwd()

# actual flag
folder = "/tmp/output"

# env var turned into config
image = "812206152185.dkr.ecr.us-west-2.amazonaws.com/test-validation:v0.0.16"

entrypoint_path = os.path.abspath(flytekit.__file__)
# black magic?
if entrypoint_path.endswith(".pyc"):
    entrypoint_path = entrypoint_path[:-1]


serialize_all(
    pkgs,
    dir,
    folder,
    SerializationMode.DEFAULT,
    image,
    config_path=None,
    flytekit_virtualenv_root=os.path.dirname(entrypoint_path),
)
