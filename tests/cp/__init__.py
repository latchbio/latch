# # variables
# # - source local/remote
# # - dest local/remote
# # - source type
# # - dest type
# # - source exists
# # - dest exists
# # - source trailing slash
# # - if source/dest remote:
# #   - domain type

# import urllib.parse
# from dataclasses import dataclass
# from enum import Enum
# from typing import Optional

# acc_id = "6612"
# bucket = "latch-sdk-test-bucket"
# node_id = "22463636"


# class Domain(Enum, str):
#     infer = ""
#     account = f"{acc_id}.account"
#     shared = "shared"
#     mount = f"{bucket}.mount"
#     shared_account = f"shared.{acc_id}.account"
#     node = f"{node_id}.node"


# @dataclass(frozen=True)
# class TestCase:
#     dir: bool
#     exists: bool
#     trailing_slash: bool


# @dataclass(frozen=True)
# class RemoteTestCase(TestCase):
#     domain: Domain


# urllib.parse.uses_netloc.append("latch")
# urllib.parse.uses_relative.append("latch")


# def generate_test_case(case: TestCase) -> str:
#     scheme: str = ""
#     netloc: str = ""
#     if isinstance(case, RemoteTestCase):
#         scheme = "latch"
#         netloc = case.domain.value


# wip: todo(ayush)
