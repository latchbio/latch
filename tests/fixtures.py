import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDVUMDA6NTM6MjMuNzM1WiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0NjY5ODExMSwiZXhwIjoxNjQ2NzM0MTExfQ.OEYoUAJ2RAZjXxbbAcIzvIbiZ8yQuLHxjNYeobULw49GO0MEsvD1iLmFWahC3srH652f-MdO4FO2RJ8pfYxgsLK-ysF3PccucfOR_v5RHC4Ge8AMlMMgRQYcSyLLRo0pwsoUgkTJewyD7NzTGLydDyDSndlOWnF4vB9TKxx2AIt7M3sASemTPmErbVeDBI1-eBBpt5U3xggCYs_ogPlIYd0W9qvrHrazxGaETYK2T2vI9-ob0f7pWSKNDzMkfb4e5dw5DYJfas6rTqSxGHg0D2r3aF5GH7U0Z1tNgNSg_ncBgedVy-LkJTetcsda4SUmnep6C1odSr-B1WJ6uNhTdw"
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
