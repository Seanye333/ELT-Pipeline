"""FastAPI dependency injectors for DB sessions, clients, etc."""
from __future__ import annotations

from functools import lru_cache
from typing import Generator

from src.config.settings import OracleSettings
from src.load.oracle_loader import OracleLoader


@lru_cache(maxsize=1)
def get_oracle_loader() -> OracleLoader:
    """Singleton OracleLoader — shared across all requests."""
    return OracleLoader(OracleSettings())
