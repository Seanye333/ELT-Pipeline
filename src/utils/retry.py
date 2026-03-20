"""Exponential-backoff retry decorator built on tenacity."""
from __future__ import annotations

import functools
from typing import Any, Callable, Type

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config.logging_config import get_logger
from src.config.settings import PipelineSettings

logger = get_logger(__name__)


def with_retry(
    max_attempts: int | None = None,
    backoff_seconds: int | None = None,
    exception_types: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Decorator factory for retrying functions with exponential back-off.

    Usage:
        @with_retry(max_attempts=3, backoff_seconds=5)
        def flaky_function(): ...
    """
    config = PipelineSettings()
    attempts = max_attempts or config.retry_max_attempts
    backoff = backoff_seconds or config.retry_backoff_seconds

    def decorator(func: Callable) -> Callable:
        @retry(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=backoff, min=backoff, max=backoff * 8),
            retry=retry_if_exception_type(exception_types),
            reraise=True,
        )
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper

    return decorator
