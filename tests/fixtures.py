import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDFUMjM6NDQ6NDEuMTg2WiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0NjQ0MTM3NywiZXhwIjoxNjQ2NDc3Mzc3fQ.mBVW4_0yrMRbv0sxswNw4BZYYaej_9iEo0BtLh4IAkg4UgP6vZtr14DH7NRiBoSqoZQiJ860Li6xiA5UAuuKz13-Il0eCZepgIf1ED6oH2fMuUZBVfAGYVSLwtm8cGs7xxY3UHHJ0rkl191-7LZg2LTT5CURD5TKfdv1p3XyqoABVEQUpq9UuCekBa1NEerqVyUm0Tp-6SoLtzbnih3hQm6boXfI6icBMywDSaH4ZR7GjKRiDezmqpvhMh_ou821bJRoXXRIVpjZGiEebCuFcPaV-Pxu5s3pV0_YPHMMPx8rDRMBx3rWGN2nvvpsatMVrrEPx2G5wEeAL_oCk2eXcg"
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
