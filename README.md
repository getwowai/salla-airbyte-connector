# Salla Airbyte Connector

A custom Airbyte source connector for syncing data from Salla e-commerce platform.

## Streams

| Stream | Type | Description |
|--------|------|-------------|
| `store_info` | Full Refresh | Store information (single record) |
| `order_statuses` | Full Refresh | Order status lookup table |
| `products` | Full Refresh | Product catalog |
| `customers` | Incremental | Customer profiles (cursor: `updated_at`) |
| `orders` | Incremental | Order information (cursor: `date`) |
| `order_items` | Parent-Child | Order line items (from orders) |
| `order_shipments` | Parent-Child | Order shipments (from orders) |
| `product_variants` | Parent-Child | Product variants (from products) |
| `product_quantities` | Parent-Child | Product stock levels (from products) |

## Features

- **Global Rate Limiting**: 1 request per second to stay under Cloudflare/Salla limits
- **Incremental Sync**: Uses cursor fields for efficient syncing
- **Date Chunking**: 1-day chunks to avoid Salla's 10,000 pagination limit
- **Robust Retry**: 120-second constant backoff on 429 errors (10 retries)
- **Parent-Child Streams**: Automatically fetches related data from parent streams

## Why Python over YAML?

The declarative YAML connector has fundamental limitations:
- Rate limiter is per-partition, not global
- No access to rate limit response headers during execution
- Cannot implement true sequential processing with dynamic delays

This Python connector gives full control over request timing and backoff logic.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create config file:
   ```bash
   cp secrets/config.json.template secrets/config.json
   # Edit secrets/config.json with your credentials
   ```

## Usage

### Test the connector locally

```bash
# Show connector spec
python main.py spec

# Check connection
python main.py check --config secrets/config.json

# Discover streams
python main.py discover --config secrets/config.json

# Read records (full refresh)
python main.py read --config secrets/config.json --catalog catalog.json

# Read records (incremental with state)
python main.py read --config secrets/config.json --catalog catalog.json --state state.json
```

## Configuration

### OAuth 2.0
```json
{
  "credentials": {
    "auth_method": "oauth2.0",
    "access_token": "YOUR_ACCESS_TOKEN"
  }
}
```

### API Key
```json
{
  "credentials": {
    "auth_method": "api_key",
    "api_key": "YOUR_API_KEY"
  }
}
```

## Rate Limiting Strategy

| Layer | Limit | Handling |
|-------|-------|----------|
| Cloudflare | ~1.5 req/sec sustained | Global rate limiter at 1 req/sec |
| Salla API | 500 req/10 min (50/min) | Well under limit at 1 req/sec |
| 429 Response | No Retry-After header | 120-second constant backoff, 10 retries |

## Deploy to Airbyte

### Docker (Self-hosted Airbyte)
```bash
# Build Docker image
docker build -t source-salla-python:0.2.0 .

# Push to registry (example: GCP Artifact Registry)
docker tag source-salla-python:0.2.0 europe-docker.pkg.dev/YOUR_PROJECT/airbyte-connector/source-salla-python:0.2.0
docker push europe-docker.pkg.dev/YOUR_PROJECT/airbyte-connector/source-salla-python:0.2.0

# Add to Airbyte as custom connector using the image URL
```

## Stream Details

### Simple Streams (Full Refresh)

- **store_info**: Basic store information, single record
- **order_statuses**: Lookup table for order status values
- **products**: Full product catalog with images, categories, variants

### Incremental Streams

- **customers**: Customer profiles with date-based filtering (`date_from`, `date_to`)
- **orders**: Order data with date-based filtering (`from_date`, `to_date`)

### Parent-Child Streams

These streams fetch data for each record in their parent stream:

- **order_items**: Line items for each order (`/orders/items?order_id={id}`)
- **order_shipments**: Shipments for each order (`/orders/{id}/shipments`)
- **product_variants**: Variants for each product (`/products/{id}/variants`)
- **product_quantities**: Stock levels for each product (`/products/{id}/quantities`)
