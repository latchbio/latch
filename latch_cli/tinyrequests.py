import json as _json
import time
from http import HTTPStatus
from http.client import HTTPException, HTTPResponse, HTTPSConnection
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse


class TinyResponse:
    def __init__(self, resp: HTTPResponse, url: str, *, stream: bool = False) -> None:
        self._resp = resp
        self._content = None
        self._url = url

        self._stream = stream
        if not self._stream:
            self._content = self._resp.read()

    @property
    def headers(self):
        return self._resp.headers

    @property
    def status_code(self):
        return self._resp.status

    def json(self):
        return _json.loads(self.content)

    @property
    def content(self):
        if self._content is None:
            self._content = self._resp.read()
        return self._content

    def iter_content(self, chunk_size: Optional[int] = 1):
        while True:
            if chunk_size is None:
                x = self._resp.read1()
            else:
                x = self._resp.read(chunk_size)

            if len(x) == 0:
                yield x
                break

            yield x

    @property
    def url(self):
        return self._url

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            err_type = "Client" if self.status_code < 500 else "Server"

            status_enum = HTTPStatus(self.status_code)
            raise HTTPException(
                f"{self.status_code} {err_type} error: {status_enum.phrase} {self.url}"
            )

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if self._stream:
            self._resp.close()


_cache: Dict[str, HTTPSConnection] = {}


def _req(
    method: str,
    url: str,
    *,
    headers: Dict[str, str] = {},
    data: Optional[bytes] = None,
    json: Optional[Any] = None,
    stream: bool = False,
):
    parts = urlparse(url)
    if parts.hostname is None:
        raise ValueError(f"could not extract hostname from {url}")

    body = None
    if data is not None:
        body = data

    if json is not None:
        body = _json.dumps(json)
        headers["Content-Type"] = "application/json"

    port = parts.port if parts.port is not None else 443
    key = f"{parts.hostname}:{port}"

    # ayush: this is not threadsafe (as in the connection could be created
    # multiple times) but its probably fine
    if _cache.get(key) is None:
        _cache[key] = HTTPSConnection(parts.hostname, port, timeout=90)

    retries = 3
    while True:
        conn = _cache[key]

        try:
            conn.request(
                method,
                urlunparse(parts._replace(scheme="", netloc="")),
                headers=headers,
                body=body,
            )
            resp = conn.getresponse()
            break
        except ConnectionError as e:
            _cache[key] = HTTPSConnection(parts.hostname, port, timeout=90)

            retries += 1
            if retries > 3:
                raise e

    return TinyResponse(resp, url, stream=stream)


def request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = {},
    data: Optional[Any] = None,
    json: Optional[bytes] = None,
    stream: bool = False,
    num_retries: int = 3,
) -> TinyResponse:
    """
    Send HTTP request. Retry on 500s or ConnectionErrors.
    Implements exponential backoff between retries.
    """
    err = None
    res = None

    attempt = 0
    while attempt < num_retries:
        res = None
        attempt += 1
        try:
            res = _req(
                method, url, headers=headers, data=data, json=json, stream=stream
            )
            if res.status_code < 500:
                return res
        except ConnectionError as e:
            err = e

        if attempt < num_retries:
            # todo(rahul): tune the sleep interval based on the startup time of the server
            # todo(rahul): change sleep interval based on which service we are calling
            time.sleep(2**attempt * 5)

    if res is None:
        raise err
    return res


def get(
    url: str,
    *,
    headers: Dict[str, str] = {},
    data: Optional[bytes] = None,
    json: Optional[Any] = None,
    stream: bool = False,
    num_retries: int = 3,
) -> TinyResponse:
    assert data is None or json is None, "At most one of `data` and `json` can be set"

    return request(
        "GET",
        url,
        headers=headers,
        data=data,
        json=json,
        stream=stream,
        num_retries=num_retries,
    )


def put(
    url: str,
    *,
    headers: Dict[str, str] = {},
    data: Optional[bytes] = None,
    json: Optional[Any] = None,
    stream: bool = False,
    num_retries: int = 3,
) -> TinyResponse:
    assert data is None or json is None, "At most one of `data` and `json` can be set"

    return request(
        "PUT",
        url,
        headers=headers,
        data=data,
        json=json,
        stream=stream,
        num_retries=num_retries,
    )


def post(
    url: str,
    *,
    headers: Dict[str, str] = {},
    data: Optional[bytes] = None,
    json: Optional[Any] = None,
    stream: bool = False,
    num_retries: int = 3,
) -> TinyResponse:
    assert data is None or json is None, "At most one of `data` and `json` can be set"

    return request(
        "POST",
        url,
        headers=headers,
        data=data,
        json=json,
        stream=stream,
        num_retries=num_retries,
    )
