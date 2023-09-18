# variables
# - source local/remote
# - dest local/remote
# - source type
# - dest type
# - source exists
# - dest exists
# - source trailing slash
# - if source/dest remote:
#   - domain type

from dataclasses import dataclass
from enum import Enum
from typing import Optional

acc_id = "6612"
bucket = "latch-sdk-test-bucket"
node_id = "22463636"


class Domain(Enum, str):
    infer = ""
    account = f"{acc_id}.account"
    shared = "shared"
    mount = f"{bucket}.mount"
    shared_account = f"shared.{acc_id}.account"
    node = f"{node_id}.node"


@dataclass(frozen=True)
class TestCase:
    remote: bool
    dir: bool
    exists: bool
    trailing_slash: bool = False
    domain: Domain = Domain.infer


def generate_test_case(src: TestCase, dest: TestCase):
    s = ""
    if src.remote:
        s = f"latch://{src.domain}/"
        if src.exists:
            ...
