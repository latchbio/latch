import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDVUMDA6NTM6MjMuNzM1WiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0NjQ0MzcxNywiZXhwIjoxNjQ2NDc5NzE3fQ.JSHpBdS1r1SUs9jSSeDV1JcOZpuM8fJ_WeZiT0o0GbXrIDJnmNo712HNZ5oa0kMScRNaY3MFvPLWe60qoovHB5uo6cd-xm9j68OHN7WEgCU1QJyECii5a_fo4-iw1Lbca5UvupDsxR0WL2Tc3A9YAqr4N9lwoQuKEkH_EWpqcDQtQYsDCoOs4HbWpk2EoF3wKSJWcLIsdDwCwxdL-6R-gyyZiqq_jii0u70naYFCcoHBeDoYghnG91EQaFHc3Lqmt4deloaeg0_uhFwBxHIJvuuLg8DVPaH5CQjPhYYXVBBWgBjoOgN0t0C3sfeNESEH7llwroiGhnhJJE38DPb_7A"
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
