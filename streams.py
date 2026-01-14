"""
Salla Streams

Implements streams for the Salla connector:
- CustomersStream: Customer profiles
- OrdersStream: Order information

Common features:
- Global rate limiting (1 req/sec)
- 1-day date chunking to avoid pagination limits
- Incremental sync via cursor fields
- 120-second backoff on 429 errors (10 retries)
"""

import logging
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping, MutableMapping, Optional, List

import requests
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode

from rate_limiter import get_rate_limiter

logger = logging.getLogger("airbyte")


def extract_datetime_string(value: any) -> str:
    """
    Extract datetime string from Salla's datetime format.

    Salla returns dates as either:
    - A string: "2025-12-22 15:25:14"
    - A dict: {"date": "2025-12-22 15:25:14.000000", "timezone_type": 3, "timezone": "Asia/Riyadh"}

    Returns the date string, or empty string if not found.
    """
    if isinstance(value, dict):
        return value.get("date", "")[:19] if value.get("date") else ""
    elif isinstance(value, str):
        return value[:19] if value else ""
    return ""


class BaseSallaStream(HttpStream):
    """
    Base class for Salla streams.

    Provides common functionality:
    - Global rate limiting at 1 req/sec
    - 120-second constant backoff on 429 (10 retries max)
    - 1-day date chunking to avoid pagination limits
    - Common pagination and response parsing
    """

    # Stream configuration
    url_base = "https://api.salla.dev/admin/v2/"
    primary_key = "id"

    # Pagination settings
    page_size = 50  # Salla max per_page

    # Rate limiting - 120 second backoff on 429 (no Retry-After from Salla)
    _backoff_seconds = 120  # seconds

    @property
    def max_retries(self) -> int:
        """Override CDK default of 5 retries to allow more attempts on rate limits."""
        return 10

    # Date chunking - 1 day chunks to avoid pagination limit
    chunk_days = 1
    start_date = "2025-01-01"  # Fetch data from 2025

    # Subclasses must define these
    cursor_field: str = None

    def __init__(
        self, authenticator: TokenAuthenticator, config: Mapping[str, Any], **kwargs
    ):
        super().__init__(authenticator=authenticator, **kwargs)
        self.config = config
        self._rate_limiter = get_rate_limiter(requests_per_second=1.0)
        self._cursor_value: Optional[str] = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Stream name."""
        pass

    @abstractmethod
    def path(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        """API endpoint path."""
        pass

    @property
    @abstractmethod
    def date_from_param(self) -> str:
        """Name of the date_from query parameter."""
        pass

    @property
    @abstractmethod
    def date_to_param(self) -> str:
        """Name of the date_to query parameter."""
        pass

    @abstractmethod
    def get_json_schema(self) -> Mapping[str, Any]:
        """Return the JSON schema for the stream."""
        pass

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """Build request parameters including pagination and date filters."""
        # RATE LIMITING: Wait before each request (1 req/sec)
        # This is called by CDK before every HTTP request
        wait_time = self._rate_limiter.wait()

        # Get stats and log rate info
        stats = self._rate_limiter.get_stats()
        page = next_page_token.get("page", 1) if next_page_token else 1
        slice_info = (
            f"{stream_slice.get('date_from', '?')} to {stream_slice.get('date_to', '?')}"
            if stream_slice
            else "no slice"
        )

        # Log every request with rate info
        logger.info(
            f"[{self.name.upper()} REQ #{stats['request_count']}] page={page}, slice={slice_info} | "
            f"Rate: {stats['recent_rate']:.2f} req/s (target: {stats['target_rate']}) | "
            f"Total: {stats['request_count']} reqs in {stats['elapsed_seconds']}s = {stats['overall_rate']:.2f} req/s"
        )

        if wait_time > 0:
            logger.debug(f"Rate limiter waited {wait_time:.2f}s")

        params = {
            "per_page": self.page_size,
        }

        # Pagination
        if next_page_token:
            params["page"] = next_page_token.get("page", 1)
        else:
            params["page"] = 1

        # Date filtering from slice (using stream-specific param names)
        if stream_slice:
            params[self.date_from_param] = stream_slice.get(
                "date_from", self.start_date
            )
            params[self.date_to_param] = stream_slice.get(
                "date_to", datetime.now().strftime("%Y-%m-%d")
            )

        return params

    def request_headers(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        Determine next page token from response.

        Salla pagination: per_page * page <= 10,000
        With 1-day chunks, we should stay well under this limit.
        """
        data = response.json()
        pagination = data.get("pagination", {})

        current_page = pagination.get("currentPage", 1)
        total_pages = pagination.get("totalPages", 1)

        # Check pagination limit (per_page * page <= 10000)
        next_page = current_page + 1
        if next_page <= total_pages and (self.page_size * next_page) <= 10000:
            return {"page": next_page}

        return None

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Parse records from response and track cursor."""
        data = response.json()
        records = data.get("data", [])

        for record in records:
            # Track max cursor value (extract string from dict if needed)
            record_cursor = extract_datetime_string(record.get(self.cursor_field))
            if record_cursor:
                if self._cursor_value is None or record_cursor > self._cursor_value:
                    self._cursor_value = record_cursor

            yield record

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Generate date-based slices (1-day chunks).

        For incremental sync, start from cursor state.
        For full refresh, start from start_date.
        """
        # Determine start date
        if sync_mode == SyncMode.incremental and stream_state:
            # Start from cursor with 1-day lookback
            cursor_value = stream_state.get(self.cursor_field)
            if cursor_value:
                try:
                    start = datetime.strptime(
                        cursor_value[:10], "%Y-%m-%d"
                    ) - timedelta(days=1)
                except (ValueError, TypeError):
                    start = datetime.strptime(self.start_date, "%Y-%m-%d")
            else:
                start = datetime.strptime(self.start_date, "%Y-%m-%d")
        else:
            start = datetime.strptime(self.start_date, "%Y-%m-%d")

        end = datetime.now()

        # Generate 1-day chunks
        current = start
        while current < end:
            chunk_end = min(current + timedelta(days=self.chunk_days), end)

            yield {
                "date_from": current.strftime("%Y-%m-%d"),
                "date_to": chunk_end.strftime("%Y-%m-%d"),
            }

            current = chunk_end + timedelta(days=1)

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        """Update state with latest cursor value."""
        current_cursor = current_stream_state.get(self.cursor_field, "")
        # Extract string from dict if Salla returns nested datetime object
        record_cursor = extract_datetime_string(latest_record.get(self.cursor_field))

        if record_cursor and record_cursor > current_cursor:
            return {self.cursor_field: record_cursor}

        return current_stream_state

    @property
    def state(self) -> MutableMapping[str, Any]:
        """Get current state."""
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        return {}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        """Set state from checkpoint."""
        self._cursor_value = value.get(self.cursor_field)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """
        Return backoff time in seconds for rate limit handling.

        Called by Airbyte CDK when a retryable error occurs.
        Returns constant 120 seconds for 429 errors (Salla doesn't send Retry-After).
        """
        if response.status_code == 429:
            return float(self._backoff_seconds)
        return None


class CustomersStream(BaseSallaStream):
    """
    Salla Customers Stream

    Endpoint: GET /customers
    Date params: date_from, date_to
    Cursor: updated_at
    """

    cursor_field = "updated_at"

    @property
    def name(self) -> str:
        return "customers"

    def path(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "customers"

    @property
    def date_from_param(self) -> str:
        return "date_from"

    @property
    def date_to_param(self) -> str:
        return "date_to"

    def get_json_schema(self) -> Mapping[str, Any]:
        """Return the JSON schema for customers."""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Customer ID"},
                "first_name": {
                    "type": ["string", "null"],
                    "description": "Customer first name",
                },
                "last_name": {
                    "type": ["string", "null"],
                    "description": "Customer last name",
                },
                "email": {"type": ["string", "null"], "description": "Customer email"},
                "mobile": {
                    "type": ["string", "null"],
                    "description": "Customer mobile number",
                },
                "mobile_code": {
                    "type": ["string", "null"],
                    "description": "Mobile country code",
                },
                "city": {"type": ["string", "null"], "description": "Customer city"},
                "country": {
                    "type": ["string", "null"],
                    "description": "Customer country",
                },
                "currency": {
                    "type": ["string", "null"],
                    "description": "Preferred currency",
                },
                "avatar": {
                    "type": ["string", "null"],
                    "description": "Customer avatar URL",
                },
                "gender": {
                    "type": ["string", "null"],
                    "description": "Customer gender",
                },
                "groups": {"type": ["array", "null"], "description": "Customer groups"},
                "orders_count": {
                    "type": ["integer", "null"],
                    "description": "Total orders placed",
                },
                "total_spent": {
                    "type": ["number", "null"],
                    "description": "Total amount spent",
                },
                "created_at": {
                    "type": ["string", "null"],
                    "description": "Creation timestamp",
                },
                "updated_at": {
                    "type": ["string", "null"],
                    "description": "Last update timestamp",
                },
            },
            "additionalProperties": True,
        }


class OrdersStream(BaseSallaStream):
    """
    Salla Orders Stream

    Endpoint: GET /orders
    Date params: from_date, to_date
    Cursor: date (order date)
    """

    cursor_field = "date"

    @property
    def name(self) -> str:
        return "orders"

    def path(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "orders"

    @property
    def date_from_param(self) -> str:
        return "from_date"

    @property
    def date_to_param(self) -> str:
        return "to_date"

    def get_json_schema(self) -> Mapping[str, Any]:
        """Return the JSON schema for orders."""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Order ID"},
                "reference_id": {
                    "type": ["integer", "null"],
                    "description": "Order reference number",
                },
                "date": {
                    "type": ["string", "object", "null"],
                    "description": "Order date",
                },
                "status": {
                    "type": ["object", "null"],
                    "description": "Order status details",
                },
                "total": {
                    "type": ["object", "null"],
                    "description": "Order total with amount and currency",
                },
                "sub_total": {
                    "type": ["object", "null"],
                    "description": "Order subtotal",
                },
                "shipping_cost": {
                    "type": ["object", "null"],
                    "description": "Shipping cost",
                },
                "cash_on_delivery": {
                    "type": ["object", "null"],
                    "description": "Cash on delivery fee",
                },
                "tax": {"type": ["object", "null"], "description": "Tax amount"},
                "discount": {
                    "type": ["object", "null"],
                    "description": "Discount amount",
                },
                "exchange_rate": {
                    "type": ["object", "null"],
                    "description": "Exchange rate information",
                },
                "payment_method": {
                    "type": ["string", "null"],
                    "description": "Payment method used",
                },
                "is_pending_payment": {
                    "type": ["boolean", "null"],
                    "description": "Whether payment is pending",
                },
                "pending_payment_ends_at": {
                    "type": ["integer", "null"],
                    "description": "When pending payment expires",
                },
                "can_cancel": {
                    "type": ["boolean", "null"],
                    "description": "Whether order can be cancelled",
                },
                "can_reorder": {
                    "type": ["boolean", "null"],
                    "description": "Whether order can be reordered",
                },
                "features": {
                    "type": ["object", "null"],
                    "description": "Order features (digitalable, shippable, etc.)",
                },
                "items": {"type": ["array", "null"], "description": "Order line items"},
                "customer": {
                    "type": ["object", "null"],
                    "description": "Customer information",
                },
                "shipping": {
                    "type": ["object", "null"],
                    "description": "Shipping information",
                },
                "urls": {"type": ["object", "null"], "description": "Related URLs"},
                "created_at": {
                    "type": ["string", "null"],
                    "description": "Creation timestamp",
                },
                "updated_at": {
                    "type": ["string", "null"],
                    "description": "Last update timestamp",
                },
            },
            "additionalProperties": True,
        }
