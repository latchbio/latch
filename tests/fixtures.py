import secrets
import string
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_account_jwt():

    # TODO: env variable
    tmp_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNMdVluMm1jTDN5UXByWjRRQ0pMdiJ9.eyJnaXZlbl9uYW1lIjoiQXl1c2giLCJmYW1pbHlfbmFtZSI6IkthbWF0Iiwibmlja25hbWUiOiJheXVzaCIsIm5hbWUiOiJBeXVzaCBLYW1hdCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQVRYQUp5a3JjWnFKZmpJZjdJbnFvbGo4enlXU3V2UVBDUDFRa3F5c2pqej1zOTYtYyIsImxvY2FsZSI6ImVuIiwidXBkYXRlZF9hdCI6IjIwMjItMDMtMDlUMDc6MTI6MjIuMDMyWiIsImVtYWlsIjoiYXl1c2hAbGF0Y2guYmlvIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vbGF0Y2hhaS51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDE0OTQ5MDA1MTY1ODkzMjM0MjkiLCJhdWQiOiJqekZCT2hJYmZwNEVQUllaOHdteDRZeXZMMjdMRkRlQiIsImlhdCI6MTY0Njg3MzAwMiwiZXhwIjoxNjQ2OTA5MDAyfQ.K-qLK3v2QMUDrnmC5X71vpE7XWMYZsvApPUlp4d3_CPflf6scY7ZHKUr_Y1YMmCDmj78sscynKJPsWaos8G4lps1GpcbSVqDR6vHTYh6Tz0I2ugYeB8ZpAtFrKjm56wLEx7t2iYgDJ6iyJgr3hpB-4YDk3SbeWaVTGsdvqQfBWH7kH4S2x1qUOOW3_OLdYiM1EiE8LbSQb2rlvM5lrTk-EI7BXXCdCKpUUlU1joR1FqRwmnnQ4M-_XA80vEgJlJMg3O99Ve82Rx2S6AbKVu4DH5R7the9qJnu9C1pa_bYjkNh2wLlW1AYD9n6li3tNYN7gexlutV7DxRDI9WJaG7Fg"
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
