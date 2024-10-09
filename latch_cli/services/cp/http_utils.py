import asyncio
from http import HTTPStatus
from typing import Awaitable, Callable, List, Optional

import aiohttp
from typing_extensions import ParamSpec

P = ParamSpec("P")


class RetriesExhaustedException(RuntimeError): ...


class RetryClientSession(aiohttp.ClientSession):
    def __init__(
        self,
        status_list: Optional[List[HTTPStatus]] = None,
        retries: int = 3,
        backoff: float = 1,
        *args,
        **kwargs,
    ):

        self.status_list = (
            status_list
            if status_list is not None
            else [
                HTTPStatus.TOO_MANY_REQUESTS,  # 429
                HTTPStatus.INTERNAL_SERVER_ERROR,  # 500
                HTTPStatus.BAD_GATEWAY,  # 502
                HTTPStatus.SERVICE_UNAVAILABLE,  # 503
                HTTPStatus.GATEWAY_TIMEOUT,  # 504
            ]
        )

        self.retries = retries
        self.backoff = backoff

        super().__init__(*args, **kwargs)

    async def _with_retry(
        self,
        f: Callable[P, Awaitable[aiohttp.ClientResponse]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> aiohttp.ClientResponse:
        error: Optional[Exception] = None

        cur = 0
        while cur < self.retries:
            if cur > 0:
                await asyncio.sleep(self.backoff * 2**cur)

            cur += 1

            try:
                res = await f(*args, **kwargs)
                if res.status in self.status_list:
                    continue

                return res
            except Exception as e:
                error = e
                continue

        if error is None:
            raise RetriesExhaustedException

        raise error

    async def _request(self, *args, **kwargs) -> aiohttp.ClientResponse:
        return await self._with_retry(super()._request, *args, **kwargs)
