import asyncio
import functools
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import floor
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
    Polls an asynchronous function at regular intervals until it returns
    a result or a timeout is reached.

    Args:
        fn (Callable[[], R]): The asynchronous function to be polled.
        options (PollingOptions): The polling options.
        stop_condition (Callable[[R], bool] | None): An optional function
            that determines when to stop polling based on the result. If
            None, polling will continue until success or timeout or retry
            limit exceeded. (default: None)
    Returns:
        PollingResult[R]: The result or error returned by the function,
            wrapped in polling result.

    Example:
        >>> async def fetch_data() -> int:
        ...     # Simulate an asynchronous operation
        ...     await asyncio.sleep(1)
        ...     return 42
        ...
        >>> options = PollingOptions(
        ...     interval=500,  # 500 milliseconds
        ...     timeout=5000,  # 5 seconds
        ...     backoff_factor=2,
        ...     retry_limit=10
        ... )
        >>> result = await poll(fetch_data, options, stop_condition=lambda x: x == 42)
        >>> if isinstance(result, PollSuccess):
        ...     print(f"Success: {result.result} after {result.attempts} attempts")
        ... else:
        ...     print(f"Failure: {result.error} after {result.attempts} attempts
        ...
        Success: 42 after 1 attempts
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
    Polls a synchronous function at regular intervals until it returns
    a result or a timeout is reached.

    Args:
        fn (Callable[[], R]): The synchronous function to be polled.
        options (PollingOptions): The polling options.
        stop_condition (Callable[[R], bool] | None): An optional function
            that determines when to stop polling based on the result. If
            None, polling will continue until success or timeout or retry
            limit exceeded. (default: None)
    Returns:
        PollingResult[R]: The result returned by the function, wrapped in polling result.
    """

    async def wrapped_fn() -> R:
        return fn()

    return asyncio.run(poll(wrapped_fn, options, stop_condition))


type Decorator[**P, R] = Callable[[Callable[P, R]], Callable[P, R]]


class Cache[T](TypedDict):
    value: T
    expiry: datetime | None


@overload
def memomize_with_ttl[**P, R](
    fn: Callable[P, R], /, *, ttl: timedelta | None
) -> Callable[P, R]: ...
@overload
def memomize_with_ttl[**P, R](
    fn: None = None, /, *, ttl: timedelta | None
) -> Decorator[P, R]: ...


def memomize_with_ttl[**P, R](
    fn: Callable[P, R] | None = None,
    /,
    *,
    ttl: timedelta | None = None,
) -> Decorator[P, R] | Callable[P, R]:
    """
    Memoization decorator with optional time-to-live (TTL).

    Args:
        fn (Callable[P, R] | None): The function to be memoized.
        ttl (timedelta | None): The time-to-live for the cached value.
            If None, the value is cached indefinitely.

    Returns:
        Decorator[P, R] | Callable[P, R]: The memoized function or a
            decorator that can be applied to a function.

    Example:
        >>> @memomize_with_ttl(ttl=timedelta(seconds=60))
        ... def expensive_function(x: int) -> int:
        ...    # Expensive computation here
        ...    return x * x
        ...

        >>> # cache results indefinitely
        >>> @memomize_with_ttl(ttl=None)  # same as using @memoize
        ... def another_expensive_function(z: int) -> int:
        ...    # Expensive computation here
        ...    return z + z
        ...

        >>> def yet_another_function(a: int) -> int:
        ...    # Expensive computation here
        ...    return a - a
        ...

        >>> memoized_yet_another_function = memomize_with_ttl(
        ...    yet_another_function,
        ...    ttl=timedelta(minutes=5)
        ... )
    """
    cache: Cache[R] | None = None

    def wrapper(fn: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(fn)
        def func(*args: P.args, **kwargs: P.kwargs) -> R:
            nonlocal cache
            now = datetime.now()
            if cache and (cache["expiry"] is None or now < cache["expiry"]):
                return cache["value"]
            result = fn(*args, **kwargs)
            expiry = None if ttl is None else now + ttl
            cache = {"value": result, "expiry": expiry}
            return result

        return func

    return wrapper if fn is None else wrapper(fn)


memoize = functools.partial(memomize_with_ttl, ttl=None)
