import asyncio
from http import HTTPStatus
from typing import Awaitable, Callable, List, Optional

import aiohttp
from typing_extensions import ParamSpec

P = ParamSpec("P")


class RetriesExhaustedException(RuntimeError): ...


class RateLimitExceeded(RuntimeError): ...


class RetryClientSession(aiohttp.ClientSession):
    def __init__(
        self,
        status_list: Optional[List[HTTPStatus]] = None,
        retries: int = 10,
        backoff: float = 0.1,
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
        last_res: Optional[aiohttp.ClientResponse] = None

        cur = 0
        while cur < self.retries:
            if cur > 0:
                await asyncio.sleep(max(self.backoff * 2**cur, 10))

            cur += 1

            try:
                res = await f(*args, **kwargs)
                if res.status in self.status_list:
                    last_res = res
                    continue

                return res
            except Exception as e:
                error = e
                continue

        if last_res is not None:
            return last_res

        if error is not None:
            raise error

        # we'll never get here but putting here anyway so the type checker is happy
        raise RetriesExhaustedException

    async def _request(self, *args, **kwargs) -> aiohttp.ClientResponse:
        return await self._with_retry(super()._request, *args, **kwargs)
