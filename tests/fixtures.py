import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDFUMjM6NDQ6NDEuMTg2WiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0NjM1NDY5NSwiZXhwIjoxNjQ2MzkwNjk1fQ.Yzm1jZOx8LugO9tT9VSmQsEjeNTQJehsMxwSr0N_eERT9BTU68IZlDl5E_e_EoWDjs6O3eMuwkSVKBfRluzuu3jS1YkP1kBpm3B9kAM3NK4wa9XOE-gaw4OrcVyBozEZNw6dahZR4GpCzSvwalaeJy6gH3SFZOzSuinNX_nx0oV11noaxbD5RoM5WRvEBVpC3iD7vLSGaVd6Qmvhg7QBqhm9Bad_b8Kam8NMGUatDIhWVMJZyHA7Zgw-rb_xal7hU6oWU875DlgbqqX52JM1w_meb5x_C3DauiiIs-mlwdEfhkxL78znpBntUBc_iOyLE1eRI4mDPj13sj1oxlUE4w"
    token_dir = Path.home().joinpath(".latch")
    token_dir.mkdir(exist_ok=True)
    token_file = token_dir.joinpath("token")
    with open(token_file, "w") as f:
        f.write(tmp_token)

    return tmp_token


@pytest.fixture(scope="session")
def project_name():
    alphabet = string.ascii_letters
    return "".join(secrets.choice(alphabet) for i in range(8)).lower()
