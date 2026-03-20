"""
Microsoft Graph API OAuth2 client using client credentials flow (MSAL).
Handles token acquisition, caching, and automatic refresh.
"""
from __future__ import annotations

import time
from typing import Any

import msal
import requests

from src.config.logging_config import get_logger
from src.config.settings import GraphAPISettings

logger = get_logger(__name__)


class GraphAPIClient:
    """Authenticated client for Microsoft Graph API v1.0."""

    TOKEN_CACHE_BUFFER_SECONDS = 300  # Refresh 5 min before expiry

    def __init__(self, config: GraphAPISettings | None = None) -> None:
        self._config = config or GraphAPISettings()
        self._app = msal.ConfidentialClientApplication(
            client_id=self._config.client_id,
            client_credential=self._config.client_secret,
            authority=f"https://login.microsoftonline.com/{self._config.tenant_id}",
        )
        self._token: str | None = None
        self._token_expiry: float = 0.0
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _acquire_token(self) -> str:
        """Acquire or return cached access token."""
        if self._token and time.time() < self._token_expiry:
            return self._token

        logger.info("acquiring_graph_token", tenant=self._config.tenant_id)
        result = self._app.acquire_token_for_client(scopes=[self._config.scope])

        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "unknown"))
            raise RuntimeError(f"Failed to acquire Graph API token: {error}")

        self._token = result["access_token"]
        expires_in = result.get("expires_in", 3600)
        self._token_expiry = time.time() + expires_in - self.TOKEN_CACHE_BUFFER_SECONDS
        logger.info("graph_token_acquired", expires_in=expires_in)
        return self._token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._acquire_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET request to Graph API. Raises on non-2xx."""
        url = f"{self._config.base_url}/{path.lstrip('/')}"
        logger.debug("graph_get", url=url, params=params)
        resp = self._session.get(url, headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_bytes(self, path: str) -> bytes:
        """GET binary content (file download)."""
        url = f"{self._config.base_url}/{path.lstrip('/')}"
        logger.debug("graph_get_bytes", url=url)
        resp = self._session.get(url, headers=self._headers(), timeout=120, stream=True)
        resp.raise_for_status()
        return resp.content

    def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict]:
        """Collect all pages from a paginated Graph API endpoint."""
        results: list[dict] = []
        url: str | None = f"{self._config.base_url}/{path.lstrip('/')}"
        while url:
            logger.debug("graph_paginate", url=url)
            resp = self._session.get(url, headers=self._headers(), params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
            params = None  # nextLink already contains query params
        return results

    def check_connectivity(self) -> bool:
        """Verify we can authenticate and reach the Graph API."""
        try:
            self.get("me") if not self._config.user_id else self.get(
                f"users/{self._config.user_id}"
            )
            return True
        except Exception as exc:
            logger.warning("graph_connectivity_check_failed", error=str(exc))
            return False
