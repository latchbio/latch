import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDlUMDc6MTI6MjIuMDMyWiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0NjgwOTk0MiwiZXhwIjoxNjQ2ODQ1OTQyfQ.O6kzfyaT3pcEB-DEmyR3glEl6dpi3PFGelqcCAEAPUwdabP3WBryO9BXEdlzd5PyUWFDxShZwaNLY9JSBjQicFZ1Mq32FE7PgIQHozpuBgvCWLCpiblBE7RQWYLGUz8l64NoXIIUzGkVbxjPyJoBdP_Z6OXuyNExxLWU6Vgjl0s5rzz_PEL4lr1b4Gol0x5CkvXjnzDtEDBM1fiCNS9N1Y8_gZTf9t-02Kj_SzQGdFCJOSvS-ZUUGUsOXIhBocVSqoOfdGlrYwsObDNtTqDZ0w-iYwtcajJ8_ENGMN2lBQKkqmAXe5GvKbvu-T35KKGwgEPpyuqFjIQ0YIzj3AlXAg"
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
