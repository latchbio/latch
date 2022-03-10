import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDlUMDc6MTI6MjIuMDMyWiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0Njg3MTAwNSwiZXhwIjoxNjQ2OTA3MDA1fQ.MZC4RIdZkkGjOk68DkJg-ixUI4zTQXvNi3fyYW7fPYv2az-8mObm4HfkgDn6Lc_4EJeKBQVfhQ2dA1EEIXqOT2FETHFYjOZ14_TU2p3FHDRa2mtapAjS1CRMx56SEA9u0sKad0YDTJWqcH4rJNmERVagBG19nilecNpUh9u5rSAY1BqhZbu6oLH56KbsDHa0k2FBblA2fWnJUTnnSWzmbrqio46LOZlwatjw6isE99ZDPA7iUL7dfAaMBZ5fd9jWy0LMME3gqtzGTQ7kbz5KlQjVGl5bl60SmsmDWD2Hxrj2TAdfVfcHTyDvM_M6FQedBX8DTlo997HvQlbdiTYnmQ"
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
