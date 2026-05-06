# Orders ETL Integration

This guide explains how to integrate orders data from PostgreSQL into your ETL pipeline for incremental loading capabilities.

## Overview

Your ETL pipeline now supports three data sources:
- **Users & Products**: From DummyJSON API (full loads)
- **Orders**: From PostgreSQL database (incremental loads with timestamps)

## Database Setup

### 1. Start the Ecommerce Database

```bash
# Start all services including the ecommerce database
docker compose up -d

# Or start just the database
docker compose up -d ecommerce-db
```

### 2. Set Up Sample Data

Run the database setup script to create tables and populate sample orders:

```bash
# Run the setup script
python setup_sample_database.py
```

This will:
- Create an `orders` table with proper indexes
- Generate 200 sample orders with realistic timestamps
- Set up data spanning the last year for incremental testing

### 3. Verify Database Connection

```bash
# Connect to the database
docker exec -it ecommerce-db psql -U postgres -d ecommerce

# Check orders data
SELECT COUNT(*) as total_orders,
       MIN(updated_at) as earliest,
       MAX(updated_at) as latest
FROM orders;
```

## ETL Pipeline Updates

### New Components Added:

1. **Database Extractor** (`etl/extractors/db_extractor.py`)
   - Connects to PostgreSQL
   - Supports incremental extraction using `updated_at` timestamps
   - Handles JSON product arrays

2. **Orders Transformer** (`etl/transformers/transformer.py`)
   - Cleans and enriches order data
   - Flattens product arrays
   - Calculates derived metrics (discount amounts, averages)

3. **Orders Loader** (`etl/loaders/snowflake_loader.py`)
   - `load_raw_orders()` function for upsert operations
   - Uses `order_id` as merge key

4. **DAG Updates**:
   - **Main DAG**: Daily full loads of all three datasets
   - **Incremental DAG**: Hourly incremental loads using watermarks

### Pipeline Flow:

```
API Sources (DummyJSON) → PostgreSQL → Transform → Snowflake
     ↓                        ↓
  Users & Products        Orders (with timestamps)
     ↓                        ↓
  Full Loads          Incremental Loads
```

## Testing the Pipeline

### 1. Test Individual Components

```bash
# Test orders extraction
python -c "
from etl.extractors.db_extractor import extract_orders
df = extract_orders()
print(f'Extracted {len(df)} orders')
print(df.head())
"
```

### 2. Run Full ETL Test

```bash
# Test all components together
python test.py
```

This will extract from all sources, transform data, and export to CSV files.

### 3. Test Incremental Loading

```bash
# Test incremental extraction with watermark
python -c "
from datetime import datetime, timedelta
from etl.extractors.db_extractor import extract_orders

# Simulate yesterday's watermark
watermark = datetime.now() - timedelta(days=1)
df = extract_orders(watermark=watermark)
print(f'Incremental extract: {len(df)} orders since {watermark}')
"
```

## Configuration

### Environment Variables

Add these to your `.env` file:

```bash
# Database connection for orders
DB_HOST=ecommerce-db
DB_PORT=5432
DB_NAME=ecommerce
DB_USER=postgres
DB_PASSWORD=postgres
```

### Docker Services

The `docker-compose.yml` now includes:
- `postgres`: Airflow metadata database (port 5432)
- `ecommerce-db`: Source database for orders (port 5433)

## Airflow DAGs

### Main DAG (`etl_main_dag.py`)
- **Schedule**: Daily
- **Tasks**: Extract → Transform → Load for all datasets
- **Use Case**: Complete data refresh

### Incremental DAG (`incremental_load_dag.py`)
- **Schedule**: Hourly
- **Tasks**: Watermark-based incremental extraction
- **Use Case**: Real-time data updates

## Data Schema

### Orders Table Structure

```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    products JSONB NOT NULL,           -- Array of products in order
    total DECIMAL(10,2) NOT NULL,      -- Total order value
    discounted_total DECIMAL(10,2),    -- After discounts
    total_products INTEGER,            -- Number of distinct products
    total_quantity INTEGER,            -- Total items ordered
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE -- Used for incremental loading
);
```

### Sample Order Data

```json
{
  "id": 1,
  "user_id": 25,
  "products": [
    {
      "id": 15,
      "title": "Product 15",
      "price": 299.99,
      "quantity": 2,
      "total": 599.98,
      "discountPercentage": 10.0,
      "discountedPrice": 269.99
    }
  ],
  "total": 599.98,
  "discounted_total": 539.98,
  "total_products": 1,
  "total_quantity": 2,
  "created_at": "2024-03-15T10:30:00Z",
  "updated_at": "2024-03-15T11:45:00Z"
}
```

## Monitoring & Troubleshooting

### Check DAG Status

```bash
# View DAG runs
docker exec -it airflow-webserver airflow dags list

# Check specific DAG
docker exec -it airflow-webserver airflow dags show etl_incremental_pipeline
```

### Database Logs

```bash
# View ecommerce database logs
docker logs ecommerce-db

# Connect to database
docker exec -it ecommerce-db psql -U postgres -d ecommerce
```

### Common Issues

1. **Connection Refused**: Ensure `ecommerce-db` service is running
2. **No Data**: Run `python setup_sample_database.py` to populate data
3. **Import Errors**: Check that all new modules are properly installed

## Next Steps

1. **Set up Snowflake tables** for orders data
2. **Configure Snowflake connection** in Airflow
3. **Add data quality checks** and monitoring
4. **Implement alerting** for pipeline failures
5. **Add more complex transformations** as needed

Your ETL pipeline now demonstrates both full and incremental loading patterns! 🎉