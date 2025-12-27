import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import functools
from math import floor
import sys
import time
from typing import TypedDict, overload


@dataclass(frozen=True)
class PollingOptions:
    interval: int
    """Interval between polling attempts in milliseconds"""
    timeout: int
    """Maximum time to poll before timing out in milliseconds"""
    backoff_factor: int = 1
    retry_limit: int = 1


@dataclass(frozen=True)
class PollSuccess[T]:
    result: T | None = None
    attempts: int = field(kw_only=True)


@dataclass(frozen=True)
class PollFailure:
    error: Exception | None = None
    attempts: int = field(kw_only=True)


type PollingResult[T] = PollSuccess[T] | PollFailure


async def poll[R](
    fn: Callable[[], Awaitable[R]],
    options: PollingOptions,
    stop_condition: Callable[[R], bool] | None = None,
) -> PollingResult[R]:
    """
    Polls an asynchronous function at regular intervals until it returns a result or a timeout is reached.
    Args:
        ...
    Returns:
        PollingResult[R]: The result returned by the function, wrapped in polling result.
    Raises:
        TimeoutError: If the timeout is reached before the function returns a result.
    """

    retry_on_error = stop_condition is None
    attempts = 0
    current_interval = options.interval / 1000
    last_result: R | None = None
    last_error: Exception | None = None
    start_time = asyncio.get_event_loop().time()

    timeout = options.timeout / 1000

    while attempts < options.retry_limit:
        if asyncio.get_event_loop().time() - start_time >= timeout:
            if last_error and retry_on_error:
                return PollFailure(error=last_error, attempts=attempts)
            return PollSuccess(result=last_result, attempts=attempts)

        attempts += 1

        try:
            last_result = result = await fn()
            last_error = None

            if stop_condition:
                if stop_condition(result):
                    return PollSuccess(result=result, attempts=attempts)
            else:
                return PollSuccess(result=result, attempts=attempts)
        except Exception as e:
            last_error = e
            if not retry_on_error:
                return PollFailure(error=e, attempts=attempts)

        if asyncio.get_event_loop().time() - start_time >= timeout:
            if last_error and retry_on_error:
                return PollFailure(error=last_error, attempts=attempts)
            return PollSuccess(result=last_result, attempts=attempts)

        if attempts < options.retry_limit:
            await asyncio.sleep(current_interval)
            current_interval = floor(current_interval * options.backoff_factor)

    if last_error and retry_on_error:
        return PollFailure(error=last_error, attempts=attempts)
    return PollSuccess(result=last_result, attempts=attempts)


def poll_sync[R](
    fn: Callable[[], R],
    options: PollingOptions,
    stop_condition: Callable[[R], bool] | None = None,
) -> PollingResult[R]:
    """
    Polls an asynchronous function at regular intervals until it returns a result or a timeout is reached.
    Args:
        ...
    Returns:
        PollingResult[R]: The result returned by the function, wrapped in polling result.
    Raises:
        TimeoutError: If the timeout is reached before the function returns a result.
    """

    retry_on_error = stop_condition is None
    start_time = time.monotonic()
    attempts = 0
    current_interval = options.interval / 1000
    last_result: R | None = None
    last_error: Exception | None = None

    timeout = options.timeout / 1000

    while attempts < options.retry_limit:
        if time.monotonic() - start_time >= timeout:
            if last_error and retry_on_error:
                return PollFailure(error=last_error, attempts=attempts)
            return PollSuccess(result=last_result, attempts=attempts)

        attempts += 1

        try:
            last_result = result = fn()
            last_error = None

            if stop_condition:
                if stop_condition(result):
                    return PollSuccess(result=result, attempts=attempts)
            return PollSuccess(result=result, attempts=attempts)
        except Exception as e:
            last_error = e
            if not retry_on_error:
                return PollFailure(error=e, attempts=attempts)

        if time.monotonic() - start_time >= timeout:
            if last_error and retry_on_error:
                return PollFailure(error=last_error, attempts=attempts)
            return PollSuccess(result=last_result, attempts=attempts)

        if attempts < options.retry_limit:
            time.sleep(current_interval)
            current_interval = floor(current_interval * options.backoff_factor)

    if last_error and retry_on_error:
        return PollFailure(error=last_error, attempts=attempts)
    return PollSuccess(result=last_result, attempts=attempts)


type Decorator[**P, R] = Callable[[Callable[P, R]], Callable[P, R]]


class Cache[T](TypedDict):
    value: T
    expiry: datetime


@overload
def memomize_with_ttl[**P, R](fn: Callable[P, R], /, *, ttl: int) -> Callable[P, R]: ...
@overload
def memomize_with_ttl[**P, R](fn: None = None, /, *, ttl: int) -> Decorator[P, R]: ...


def memomize_with_ttl[**P, R](
    fn: Callable[P, R] | None = None,
    /,
    *,
    ttl: int,
) -> Decorator[P, R] | Callable[P, R]:
    cache: Cache[R] | None = None

    def wrapper(fn: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(fn)
        def func(*args: P.args, **kwargs: P.kwargs) -> R:
            nonlocal cache
            now = datetime.now()
            if cache and now < cache["expiry"]:
                return cache["value"]
            result = fn(*args, **kwargs)
            cache = {"value": result, "expiry": now + timedelta(seconds=ttl)}
            return result

        return func

    return wrapper if fn is None else wrapper(fn)


memoize = functools.partial(memomize_with_ttl, ttl=sys.maxsize)
