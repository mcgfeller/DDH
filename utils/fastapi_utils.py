from __future__ import annotations
import typing
import logging
import time
import asyncio
import collections.abc

import httpx
import fastapi
from frontend import sessions


async def submit1_asynch(session: sessions.Session, base_url, url, params: dict = {}):
    """ Submit one request using httpx, with a given base_url and url.
        Copy the Authorization header from the session. 
        Return the json result. 
    """
    headers = {'Authorization': 'Bearer '+session.token_str}
    async with httpx.AsyncClient(base_url=base_url, headers=headers) as client:
        j = await client.get(url, params=params)
    if j.is_error:
        try:
            msg = j.json()
        except:
            msg = str(j)
        raise fastapi.HTTPException(status_code=j.status_code, detail=msg)
    else:
        d = j.json()
        return d


def retry(fn: typing.Callable, *a, exceptions: tuple[type[Exception], ...] = (httpx.HTTPError,),
          initial_delay: int = 10, max_delay: int = 900, maxrounds: int = 10000,
          logger: logging.Logger | None = None,
          alerter: typing.Callable[[str, str]] | None = None, alert_tag: str = 'retry', alert_each: int = 5):
    """ Retry a function fn with parameters a, catching exceptions and sleeping between retries."""
    delay = initial_delay
    exception = None
    for attempt in range(max(1, maxrounds)):
        try:
            r = fn(*a)
            return r
        except exceptions as e:
            if maxrounds <= 0:  # no retries, just raise
                raise
            exception = e
            msg = f'Error {e}; sleeping {delay} seconds before retry {attempt+1}.'
            if logger:
                logger.warning(msg)
            if alerter and attempt > 0 and attempt % alert_each == 0:
                alerter(alert_tag, msg)
            time.sleep(delay)
            if delay < max_delay:
                delay = min(max_delay, delay*2)
    msg = f'Max retry attempts ({maxrounds}) exceeded, quitting!\n{exception}'
    if logger:
        logger.error(msg)
    if alerter:
        alerter(alert_tag, msg)
    if exception:
        raise exception


async def aretry(fn: typing.Callable[...,  collections.abc.Coroutine], *a, exceptions: tuple[type[Exception], ...] = (httpx.HTTPError,),
                 initial_delay: int = 10, max_delay: int = 900, maxrounds: int = 10000,
                 logger: logging.Logger | None = None,
                 alerter: typing.Callable | None = None, alert_tag: str = 'retry', alert_each: int = 5):
    """ Async version of retry. The Callable is async, so this function is async. 
        Unfortunetely, a lot of duplication due to the "blue/green" nature of Python's async.
    """
    delay = initial_delay
    for attempt in range(maxrounds):
        try:
            r = await fn(*a)
            return r
        except exceptions as e:
            if maxrounds <= 0:  # no retries, just raise
                raise
            exception = e
            msg = f'Error {e}; sleeping {delay} seconds before retry {attempt+1}.'
            if logger:
                logger.warning(msg)
            if alerter and attempt > 0 and attempt % alert_each == 0:
                alerter(alert_tag, msg)
            await asyncio.sleep(delay)
            if delay < max_delay:
                delay = min(max_delay, delay*2)
    msg = f'Max retry attempts ({maxrounds}) exceeded, quitting!\n{exception}'
    if logger:
        logger.error(msg)
    if alerter:
        alerter(alert_tag, msg)
    raise
