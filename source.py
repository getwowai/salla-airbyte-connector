"""
Salla Source

Main source class for the Salla Airbyte connector.
Implements spec, check_connection, discover, and streams methods.
"""

import logging
from typing import Any, List, Mapping, Tuple, Optional

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
import requests

from streams import (
    # Simple streams (Full Refresh)
    StoreInfoStream,
    OrderStatusesStream,
    ProductsStream,
    # Incremental streams
    CustomersStream,
    OrdersStream,
    # Parent-child streams
    OrderItemsStream,
    OrderShipmentsStream,
    ProductVariantsStream,
    ProductQuantitiesStream,
)

logger = logging.getLogger("airbyte")


class SallaSource(AbstractSource):
    """
    Salla Source

    A custom Airbyte source connector for syncing data from Salla e-commerce platform.

    Streams:
    - store_info: Store information (full refresh)
    - order_statuses: Order status lookup (full refresh)
    - products: Product catalog (full refresh)
    - customers: Customer profiles (incremental, cursor: updated_at)
    - orders: Order information (incremental, cursor: date)
    - order_items: Order line items (parent-child of orders)
    - order_shipments: Order shipments (parent-child of orders)
    - product_variants: Product variants (parent-child of products)
    - product_quantities: Product quantities (parent-child of products)

    Features:
    - OAuth 2.0 and API Key authentication
    - Incremental sync via cursor fields
    - Global rate limiting (1 req/sec)
    - 1-day date chunking for pagination limits
    - 120-second backoff on 429 errors (10 retries)
    """

    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        """
        Test connection to Salla API.

        Makes a simple request to the store/info endpoint to verify credentials.
        """
        try:
            token = self._get_token(config)

            response = requests.get(
                "https://api.salla.dev/admin/v2/store/info",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                timeout=30,
            )

            if response.status_code == 200:
                logger.info("Successfully connected to Salla API")
                return True, None
            elif response.status_code == 401:
                return (
                    False,
                    "Invalid credentials. Please check your access token or API key.",
                )
            elif response.status_code == 403:
                return False, "Access forbidden. Please check your API permissions."
            elif response.status_code == 429:
                return False, "Rate limited. Please try again in a minute."
            else:
                return (
                    False,
                    f"Connection failed with status {response.status_code}: {response.text}",
                )

        except requests.exceptions.Timeout:
            return False, "Connection timed out. Please check your network."
        except requests.exceptions.RequestException as e:
            return False, f"Connection error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Return list of available streams.

        Streams are ordered: simple first, then incremental, then parent-child.
        """
        token = self._get_token(config)
        authenticator = TokenAuthenticator(token=token)

        return [
            # Simple streams (Full Refresh)
            StoreInfoStream(authenticator=authenticator, config=config),
            OrderStatusesStream(authenticator=authenticator, config=config),
            ProductsStream(authenticator=authenticator, config=config),
            # Incremental streams
            CustomersStream(authenticator=authenticator, config=config),
            OrdersStream(authenticator=authenticator, config=config),
            # Parent-child streams
            OrderItemsStream(authenticator=authenticator, config=config),
            OrderShipmentsStream(authenticator=authenticator, config=config),
            ProductVariantsStream(authenticator=authenticator, config=config),
            ProductQuantitiesStream(authenticator=authenticator, config=config),
        ]

    def _get_token(self, config: Mapping[str, Any]) -> str:
        """
        Extract authentication token from config.

        Supports both OAuth 2.0 (access_token) and API Key authentication.
        """
        credentials = config.get("credentials", {})
        auth_method = credentials.get("auth_method", "")

        if auth_method == "oauth2.0":
            token = credentials.get("access_token")
            if not token:
                raise ValueError("Missing access_token for OAuth 2.0 authentication")
            return token
        elif auth_method == "api_key":
            token = credentials.get("api_key")
            if not token:
                raise ValueError("Missing api_key for API Key authentication")
            return token
        else:
            # Try to find token in common locations
            token = (
                credentials.get("access_token")
                or credentials.get("api_key")
                or config.get("access_token")
                or config.get("api_key")
            )
            if token:
                return token
            raise ValueError(
                f"Unknown auth_method '{auth_method}'. "
                "Please specify 'oauth2.0' or 'api_key' with corresponding credentials."
            )
