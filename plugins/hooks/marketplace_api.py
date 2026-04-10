from __future__ import annotations

from typing import Any
import time

import requests
from airflow.hooks.base import BaseHook


class MarketplaceAPIHook(BaseHook):
    conn_name_attr = "marketplace_api_conn_id"
    default_conn_name = "marketplace_api"
    conn_type = "http"
    hook_name = "Marketplace API"

    def __init__(
            self,
            marketplace_api_conn_id: str = "marketplace_api",
            timeout: int = 30,
            max_retries: int = 3,
            retry_delay: int = 2,
    ) -> None:
        super().__init__()
        self.marketplace_api_conn_id = marketplace_api_conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        print("init marketplace_api")
        conn = self.get_connection(self.marketplace_api_conn_id)

        schema = conn.schema or "http"
        host = conn.host
        port = f":{conn.port}" if conn.port else ""
        self.base_url = f"{schema}://{host}{port}".rstrip("/")

        # Conforme au cahier : password = Bearer token
        self.token = conn.password or ""

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        print("headers")
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(
            self,
            method: str,
            endpoint: str,
            params: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        last_error = None

        print("request")
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self._headers(),
                    params=params,
                    timeout=self.timeout,
                )

                # retry si erreur serveur
                if response.status_code >= 500:
                    raise requests.HTTPError(
                        f"Server error {response.status_code}: {response.text}",
                        response=response,
                    )

                response.raise_for_status()
                return response.json()

            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_error = exc
                self.log.warning(
                    "Erreur API tentative %s/%s sur %s : %s",
                    attempt,
                    self.max_retries,
                    url,
                    exc,
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise

        raise RuntimeError(f"API call failed: {last_error}")

    def get_orders(self, date: str) -> list[dict[str, Any]]:
        print("inside get orders")
        return self._request("GET", "/orders", params={"date": date})

    def get_products(self) -> list[dict[str, Any]]:
        print("inside get products")
        return self._request("GET", "/products")

    def get_sellers(self) -> list[dict[str, Any]]:
        print("inside get sellers")
        return self._request("GET", "/sellers")