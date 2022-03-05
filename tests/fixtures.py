import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDVUMDA6NTM6MjMuNzM1WiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0NjUwMDE0OCwiZXhwIjoxNjQ2NTM2MTQ4fQ.mOiuNfhorOXmNqUwOGs_phvJvv7KacDyfjyDo151eoAoiGO3FyfkNxaL6Wqh-TT6lOQEv63Qg8_kOVZHLX5JxaY5sxkP_3KXbme46Aq_eMxgJLpVanW41JItOSvaBYQxEn0vZUUifLB7WGx06WluRAGA5VZOG8Vt-m41eJR0_Olv4sOKBIOPBxhgnPSJQDwwL91ttvEgN4EieQHRJlsqcU6Yau1ZoP1VDTonix1GuTQ82nPiovRp3x_4svJ1nEAhOnHZZJ1JZzZVTrsohhW8Kh4ympMWrP6GOkxYUa_TA3YEBtAwcrjCsZuAjpVy_iberax5-sXjSGBRrDZbflGTlw"
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
