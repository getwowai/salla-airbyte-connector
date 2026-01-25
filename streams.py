"""
Salla Streams

Implements streams for the Salla connector:
- StoreInfoStream: Store information (full refresh)
- OrderStatusesStream: Order status lookup (full refresh)
- ProductsStream: Product catalog (full refresh)
- ShipmentsStream: All shipments (full refresh)
- ProductQuantitiesStream: Product quantities (full refresh)
- CustomersStream: Customer profiles (incremental)
- OrdersStream: Order information (incremental)
- OrderItemsStream: Order line items (parent-child of orders)
- ProductVariantsStream: Product variants (parent-child of products)

Common features:
- Global rate limiting (1 req/sec)
- 1-day date chunking to avoid pagination limits (for incremental streams)
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


# =============================================================================
# BASE CLASSES
# =============================================================================


class BaseSallaStream(HttpStream):
    """
    Base class for all Salla streams.

    Provides common functionality:
    - Global rate limiting at 1 req/sec
    - 120-second constant backoff on 429 (10 retries max)
    - Common pagination and response parsing
    """

    url_base = "https://api.salla.dev/admin/v2/"
    primary_key = "id"
    page_size = 50
    _backoff_seconds = 120
    
    # Class attribute (not property) - ensures CDK reads it correctly
    max_retries = 10

    def __init__(
        self, authenticator: TokenAuthenticator, config: Mapping[str, Any], **kwargs
    ):
        super().__init__(authenticator=authenticator, **kwargs)
        self.config = config
        self._rate_limiter = get_rate_limiter(requests_per_second=1.0)

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def path(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        pass

    @abstractmethod
    def get_json_schema(self) -> Mapping[str, Any]:
        pass

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

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if response.status_code == 429:
            return float(self._backoff_seconds)
        return None


class FullRefreshSallaStream(BaseSallaStream):
    """
    Base class for full refresh streams (no date filtering).
    """

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        wait_time = self._rate_limiter.wait()
        stats = self._rate_limiter.get_stats()
        page = next_page_token.get("page", 1) if next_page_token else 1

        logger.info(
            f"[{self.name.upper()} REQ #{stats['request_count']}] page={page} | "
            f"Rate: {stats['recent_rate']:.2f} req/s | "
            f"Total: {stats['request_count']} reqs in {stats['elapsed_seconds']}s"
        )

        params = {"per_page": self.page_size}
        if next_page_token:
            params["page"] = next_page_token.get("page", 1)
        else:
            params["page"] = 1

        return params

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        data = response.json()
        pagination = data.get("pagination", {})
        current_page = pagination.get("currentPage", 1)
        total_pages = pagination.get("totalPages", 1)

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
        data = response.json()
        records = data.get("data", [])
        # Handle single record response (like store_info)
        if isinstance(records, dict):
            yield records
        else:
            for record in records:
                yield record


class IncrementalSallaStream(BaseSallaStream):
    """
    Base class for incremental streams with date chunking.
    """

    chunk_days = 1
    default_start_date = "2025-01-01"
    cursor_field: str = None

    def __init__(
        self, authenticator: TokenAuthenticator, config: Mapping[str, Any], **kwargs
    ):
        super().__init__(authenticator=authenticator, config=config, **kwargs)
        self._cursor_value: Optional[str] = None
        # Read start_date from config, fallback to default
        self.start_date = config.get("start_date", self.default_start_date)

    @property
    @abstractmethod
    def date_from_param(self) -> str:
        pass

    @property
    @abstractmethod
    def date_to_param(self) -> str:
        pass

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        wait_time = self._rate_limiter.wait()
        stats = self._rate_limiter.get_stats()
        page = next_page_token.get("page", 1) if next_page_token else 1
        slice_info = (
            f"{stream_slice.get('date_from', '?')} to {stream_slice.get('date_to', '?')}"
            if stream_slice
            else "no slice"
        )

        logger.info(
            f"[{self.name.upper()} REQ #{stats['request_count']}] page={page}, slice={slice_info} | "
            f"Rate: {stats['recent_rate']:.2f} req/s | "
            f"Total: {stats['request_count']} reqs in {stats['elapsed_seconds']}s"
        )

        params = {"per_page": self.page_size}
        if next_page_token:
            params["page"] = next_page_token.get("page", 1)
        else:
            params["page"] = 1

        if stream_slice:
            params[self.date_from_param] = stream_slice.get(
                "date_from", self.start_date
            )
            params[self.date_to_param] = stream_slice.get(
                "date_to", datetime.now().strftime("%Y-%m-%d")
            )

        return params

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        data = response.json()
        pagination = data.get("pagination", {})
        current_page = pagination.get("currentPage", 1)
        total_pages = pagination.get("totalPages", 1)

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
        data = response.json()
        records = data.get("data", [])

        for record in records:
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
        if sync_mode == SyncMode.incremental and stream_state:
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
        current_cursor = current_stream_state.get(self.cursor_field, "")
        record_cursor = extract_datetime_string(latest_record.get(self.cursor_field))

        if record_cursor and record_cursor > current_cursor:
            return {self.cursor_field: record_cursor}
        return current_stream_state

    @property
    def state(self) -> MutableMapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        return {}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)


class ParentChildSallaStream(BaseSallaStream):
    """
    Base class for parent-child streams that iterate over parent records.
    """

    parent_stream_class = None
    parent_key = "id"
    partition_field = "parent_id"

    def __init__(
        self, authenticator: TokenAuthenticator, config: Mapping[str, Any], **kwargs
    ):
        super().__init__(authenticator=authenticator, config=config, **kwargs)
        self._authenticator = authenticator

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """Generate slices from parent stream records."""
        parent_stream = self.parent_stream_class(
            authenticator=self._authenticator, config=self.config
        )

        for parent_record in parent_stream.read_records(
            sync_mode=SyncMode.full_refresh
        ):
            parent_id = parent_record.get(self.parent_key)
            if parent_id:
                yield {self.partition_field: parent_id}

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        wait_time = self._rate_limiter.wait()
        stats = self._rate_limiter.get_stats()
        page = next_page_token.get("page", 1) if next_page_token else 1
        parent_id = stream_slice.get(self.partition_field, "?") if stream_slice else "?"

        logger.info(
            f"[{self.name.upper()} REQ #{stats['request_count']}] page={page}, parent_id={parent_id} | "
            f"Rate: {stats['recent_rate']:.2f} req/s | "
            f"Total: {stats['request_count']} reqs in {stats['elapsed_seconds']}s"
        )

        params = {"per_page": self.page_size}
        if next_page_token:
            params["page"] = next_page_token.get("page", 1)
        else:
            params["page"] = 1

        return params

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        data = response.json()
        pagination = data.get("pagination", {})
        current_page = pagination.get("currentPage", 1)
        total_pages = pagination.get("totalPages", 1)

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
        data = response.json()
        records = data.get("data", [])
        parent_id = stream_slice.get(self.partition_field) if stream_slice else None

        for record in records:
            # Add parent_id to each record
            if parent_id and self.partition_field not in record:
                record[self.partition_field] = parent_id
            yield record


# =============================================================================
# SIMPLE STREAMS (Full Refresh)
# =============================================================================


class StoreInfoStream(FullRefreshSallaStream):
    """
    Store Info Stream - Single record, no pagination.

    Endpoint: GET /store/info
    """

    @property
    def name(self) -> str:
        return "store_info"

    def path(self, **kwargs) -> str:
        return "store/info"

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        # Single record, no pagination
        return None

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Store ID"},
                "name": {"type": ["string", "null"], "description": "Store name"},
                "email": {"type": ["string", "null"], "description": "Store email"},
                "domain": {"type": ["string", "null"], "description": "Store domain"},
                "status": {"type": ["string", "null"], "description": "Store status"},
                "currency": {
                    "type": ["string", "null"],
                    "description": "Store currency",
                },
                "timezone": {
                    "type": ["string", "null"],
                    "description": "Store timezone",
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


class OrderStatusesStream(FullRefreshSallaStream):
    """
    Order Statuses Stream - Lookup table.

    Endpoint: GET /orders/statuses
    """

    @property
    def name(self) -> str:
        return "order_statuses"

    def path(self, **kwargs) -> str:
        return "orders/statuses"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Status ID"},
                "name": {"type": ["string", "null"], "description": "Status name"},
                "slug": {"type": ["string", "null"], "description": "Status slug"},
                "sort": {"type": ["integer", "null"], "description": "Sort order"},
                "color": {"type": ["string", "null"], "description": "Status color"},
                "is_default": {
                    "type": ["boolean", "null"],
                    "description": "Is default status",
                },
            },
            "additionalProperties": True,
        }


class ProductsStream(FullRefreshSallaStream):
    """
    Products Stream - Full product catalog.

    Endpoint: GET /products
    """

    @property
    def name(self) -> str:
        return "products"

    def path(self, **kwargs) -> str:
        return "products"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Product ID"},
                "sku": {"type": ["string", "null"], "description": "Product SKU"},
                "name": {"type": ["string", "null"], "description": "Product name"},
                "description": {
                    "type": ["string", "null"],
                    "description": "Product description",
                },
                "type": {"type": ["string", "null"], "description": "Product type"},
                "status": {"type": ["string", "null"], "description": "Product status"},
                "price": {"type": ["object", "null"], "description": "Product price"},
                "sale_price": {"type": ["object", "null"], "description": "Sale price"},
                "cost_price": {"type": ["number", "null"], "description": "Cost price"},
                "quantity": {
                    "type": ["integer", "null"],
                    "description": "Available quantity",
                },
                "unlimited_quantity": {
                    "type": ["boolean", "null"],
                    "description": "Is quantity unlimited",
                },
                "sold_quantity": {
                    "type": ["integer", "null"],
                    "description": "Total sold quantity",
                },
                "weight": {"type": ["number", "null"], "description": "Product weight"},
                "calories": {
                    "type": ["number", "null"],
                    "description": "Product calories",
                },
                "images": {"type": ["array", "null"], "description": "Product images"},
                "categories": {
                    "type": ["array", "null"],
                    "description": "Product categories",
                },
                "brand": {"type": ["object", "null"], "description": "Product brand"},
                "tags": {"type": ["array", "null"], "description": "Product tags"},
                "options": {
                    "type": ["array", "null"],
                    "description": "Product options",
                },
                "variants": {
                    "type": ["array", "null"],
                    "description": "Product variants",
                },
                "metadata": {
                    "type": ["object", "null"],
                    "description": "Additional metadata",
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


# =============================================================================
# INCREMENTAL STREAMS (Date-based)
# =============================================================================


class CustomersStream(IncrementalSallaStream):
    """
    Customers Stream - Incremental sync.

    Endpoint: GET /customers
    Date params: date_from, date_to
    Cursor: updated_at
    """

    cursor_field = "updated_at"

    @property
    def name(self) -> str:
        return "customers"

    def path(self, **kwargs) -> str:
        return "customers"

    @property
    def date_from_param(self) -> str:
        return "date_from"

    @property
    def date_to_param(self) -> str:
        return "date_to"

    def get_json_schema(self) -> Mapping[str, Any]:
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


class OrdersStream(IncrementalSallaStream):
    """
    Orders Stream - Incremental sync.

    Endpoint: GET /orders
    Date params: from_date, to_date
    Cursor: date
    """

    cursor_field = "date"

    @property
    def name(self) -> str:
        return "orders"

    def path(self, **kwargs) -> str:
        return "orders"

    @property
    def date_from_param(self) -> str:
        return "from_date"

    @property
    def date_to_param(self) -> str:
        return "to_date"

    def get_json_schema(self) -> Mapping[str, Any]:
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
                "total": {"type": ["object", "null"], "description": "Order total"},
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
                    "description": "COD fee",
                },
                "tax": {"type": ["object", "null"], "description": "Tax amount"},
                "discount": {
                    "type": ["object", "null"],
                    "description": "Discount amount",
                },
                "exchange_rate": {
                    "type": ["object", "null"],
                    "description": "Exchange rate info",
                },
                "payment_method": {
                    "type": ["string", "null"],
                    "description": "Payment method",
                },
                "is_pending_payment": {
                    "type": ["boolean", "null"],
                    "description": "Payment pending",
                },
                "pending_payment_ends_at": {
                    "type": ["integer", "null"],
                    "description": "Pending payment expiry",
                },
                "can_cancel": {
                    "type": ["boolean", "null"],
                    "description": "Can cancel",
                },
                "can_reorder": {
                    "type": ["boolean", "null"],
                    "description": "Can reorder",
                },
                "features": {
                    "type": ["object", "null"],
                    "description": "Order features",
                },
                "items": {"type": ["array", "null"], "description": "Order line items"},
                "customer": {
                    "type": ["object", "null"],
                    "description": "Customer info",
                },
                "shipping": {
                    "type": ["object", "null"],
                    "description": "Shipping info",
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


# =============================================================================
# PARENT-CHILD STREAMS
# =============================================================================


class OrderItemsStream(ParentChildSallaStream):
    """
    Order Items Stream - Child of Orders.

    Endpoint: GET /orders/items?order_id={id}
    """

    parent_stream_class = OrdersStream
    parent_key = "id"
    partition_field = "order_id"

    @property
    def name(self) -> str:
        return "order_items"

    def path(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "orders/items"

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        if stream_slice:
            params["order_id"] = stream_slice.get("order_id")
        return params

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Order item ID"},
                "order_id": {
                    "type": ["integer", "null"],
                    "description": "Associated order ID",
                },
                "product_id": {
                    "type": ["integer", "null"],
                    "description": "Product ID",
                },
                "sku": {"type": ["string", "null"], "description": "Product SKU"},
                "name": {"type": ["string", "null"], "description": "Product name"},
                "quantity": {
                    "type": ["integer", "null"],
                    "description": "Item quantity",
                },
                "price": {"type": ["object", "null"], "description": "Item price"},
                "total": {"type": ["object", "null"], "description": "Item total"},
                "weight": {"type": ["number", "null"], "description": "Item weight"},
                "options": {
                    "type": ["array", "null"],
                    "description": "Selected options",
                },
                "thumbnail": {
                    "type": ["string", "null"],
                    "description": "Product thumbnail URL",
                },
            },
            "additionalProperties": True,
        }


class ShipmentsStream(FullRefreshSallaStream):
    """
    Shipments Stream - All shipments (full refresh).

    Endpoint: GET /shipments
    Note: Salla API provides shipments as a flat list, not nested under orders.
    """

    @property
    def name(self) -> str:
        return "shipments"

    def path(self, **kwargs) -> str:
        return "shipments"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Shipment ID"},
                "order_id": {
                    "type": ["integer", "null"],
                    "description": "Associated order ID",
                },
                "tracking_number": {
                    "type": ["string", "null"],
                    "description": "Tracking number",
                },
                "shipping_company": {
                    "type": ["object", "null"],
                    "description": "Shipping company details",
                },
                "status": {
                    "type": ["string", "null"],
                    "description": "Shipment status",
                },
                "shipped_at": {
                    "type": ["string", "null"],
                    "description": "Shipped timestamp",
                },
                "delivered_at": {
                    "type": ["string", "null"],
                    "description": "Delivered timestamp",
                },
                "created_at": {
                    "type": ["string", "null"],
                    "description": "Creation timestamp",
                },
                "order": {
                    "type": ["object", "null"],
                    "description": "Order details",
                },
                "customer": {
                    "type": ["object", "null"],
                    "description": "Customer details",
                },
            },
            "additionalProperties": True,
        }


class ProductVariantsStream(ParentChildSallaStream):
    """
    Product Variants Stream - Child of Products.

    Endpoint: GET /products/{product_id}/variants
    """

    parent_stream_class = ProductsStream
    parent_key = "id"
    partition_field = "product_id"

    @property
    def name(self) -> str:
        return "product_variants"

    def path(
        self,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        product_id = stream_slice.get("product_id") if stream_slice else ""
        return f"products/{product_id}/variants"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Variant ID"},
                "product_id": {
                    "type": ["integer", "null"],
                    "description": "Parent product ID",
                },
                "sku": {"type": ["string", "null"], "description": "Variant SKU"},
                "barcode": {
                    "type": ["string", "null"],
                    "description": "Variant barcode",
                },
                "price": {"type": ["object", "null"], "description": "Variant price"},
                "sale_price": {
                    "type": ["object", "null"],
                    "description": "Variant sale price",
                },
                "stock_quantity": {
                    "type": ["integer", "null"],
                    "description": "Stock quantity",
                },
                "weight": {"type": ["number", "null"], "description": "Variant weight"},
                "options": {
                    "type": ["array", "null"],
                    "description": "Variant options",
                },
                "mpn": {
                    "type": ["string", "null"],
                    "description": "Manufacturer part number",
                },
                "gtin": {
                    "type": ["string", "null"],
                    "description": "Global trade item number",
                },
            },
            "additionalProperties": True,
        }


class ProductQuantitiesStream(FullRefreshSallaStream):
    """
    Product Quantities Stream - All product quantities (full refresh).

    Endpoint: GET /products/quantities
    Note: Salla API provides product quantities as a flat list, not nested under products.
    """

    @property
    def name(self) -> str:
        return "product_quantities"

    def path(self, **kwargs) -> str:
        return "products/quantities"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer", "description": "Product ID"},
                "name": {
                    "type": ["string", "null"],
                    "description": "Product name",
                },
                "image": {
                    "type": ["string", "null"],
                    "description": "Product image URL",
                },
                "sku_id": {
                    "type": ["integer", "null"],
                    "description": "SKU ID for variants",
                },
                "sku": {
                    "type": ["string", "null"],
                    "description": "Product SKU",
                },
                "quantity": {
                    "type": ["integer", "null"],
                    "description": "Available quantity",
                },
                "sold_quantity": {
                    "type": ["integer", "null"],
                    "description": "Total sold quantity",
                },
                "price": {
                    "type": ["number", "string", "null"],
                    "description": "Product price",
                },
                "unlimited_quantity": {
                    "type": ["boolean", "null"],
                    "description": "Is quantity unlimited",
                },
                "variant": {
                    "type": ["string", "null"],
                    "description": "Variant name if applicable",
                },
            },
            "additionalProperties": True,
        }
