"""
setup_sample_database.py
─────────────────────────
Sets up a sample PostgreSQL database with orders data for ETL testing.
Creates tables and populates with sample data including timestamps.
"""
import psycopg2
from datetime import datetime, timedelta
import random
import json


def create_orders_table(conn):
    """Create the orders table with sample data."""
    with conn.cursor() as cur:
        # Create orders table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            products JSONB NOT NULL,
            total DECIMAL(10,2) NOT NULL,
            discounted_total DECIMAL(10,2) NOT NULL,
            total_products INTEGER NOT NULL,
            total_quantity INTEGER NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Create index on updated_at for incremental loading
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at);
        """)

        print("Created orders table with indexes")


def generate_sample_orders(num_orders=100):
    """Generate sample orders data."""
    orders = []

    for i in range(1, num_orders + 1):
        user_id = random.randint(1, 100)  # Reference DummyJSON users

        # Generate random products for this order
        num_products_in_order = random.randint(1, 5)
        products = []
        total = 0
        total_quantity = 0

        for j in range(num_products_in_order):
            product_id = random.randint(1, 100)  # Reference DummyJSON products
            quantity = random.randint(1, 3)
            price = round(random.uniform(10, 500), 2)
            discount = round(random.uniform(0, 0.3), 2)

            product = {
                "id": product_id,
                "title": f"Product {product_id}",
                "price": price,
                "quantity": quantity,
                "total": round(price * quantity * (1 - discount), 2),
                "discountPercentage": discount * 100,
                "discountedPrice": round(price * (1 - discount), 2)
            }
            products.append(product)
            total += product["total"]
            total_quantity += quantity

        # Create order record
        created_at = datetime.now() - timedelta(days=random.randint(0, 365))
        updated_at = created_at + timedelta(hours=random.randint(0, 24))

        order = {
            "id": i,
            "user_id": user_id,
            "products": products,
            "total": round(total, 2),
            "discounted_total": round(total * 0.95, 2),  # 5% overall discount
            "total_products": len(products),
            "total_quantity": total_quantity,
            "created_at": created_at,
            "updated_at": updated_at
        }
        orders.append(order)

    return orders


def insert_sample_data(conn, orders):
    """Insert sample orders into the database."""
    with conn.cursor() as cur:
        for order in orders:
            cur.execute("""
            INSERT INTO orders (
                id, user_id, products, total, discounted_total,
                total_products, total_quantity, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """, (
                order["id"],
                order["user_id"],
                json.dumps(order["products"]),
                order["total"],
                order["discounted_total"],
                order["total_products"],
                order["total_quantity"],
                order["created_at"],
                order["updated_at"]
            ))

        conn.commit()
        print(f"Inserted {len(orders)} sample orders")


def main():
    """Main setup function."""
    # Database connection parameters
    # Note: Docker forwards port 5433 (host) -> 5432 (container)
    import os
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = int(os.environ.get("DB_PORT", "5433"))  # Use 5433 for Docker forwarded port
    
    db_config = {
        "host": db_host,
        "port": db_port,
        "database": "ecommerce",
        "user": "postgres",
        "password": "postgres"
    }

    try:
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**db_config)

        print("Creating orders table...")
        create_orders_table(conn)

        print("Generating sample orders data...")
        orders = generate_sample_orders(200)  # Generate 200 sample orders

        print("Inserting sample data...")
        insert_sample_data(conn, orders)

        print("[SUCCESS] Database setup complete!")
        print(f"Created {len(orders)} sample orders with timestamps for incremental loading.")

        # Show some sample data
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as total_orders, MIN(updated_at), MAX(updated_at) FROM orders")
            result = cur.fetchone()
            print(f"Total orders: {result[0]}")
            print(f"Date range: {result[1]} to {result[2]}")

    except Exception as e:
        print(f"[ERROR] Failed to set up database: {e}")
        return False

    finally:
        if 'conn' in locals():
            conn.close()

    return True


if __name__ == "__main__":
    success = main()
    if success:
        print("\n[SUCCESS] Your PostgreSQL database is ready for ETL testing!")
        print("You can now run your Airflow DAGs to extract orders data.")
    else:
        print("\n[FAILED] Database setup failed. Please check:")
        print("  1. Is the ecommerce-db service running? (docker compose up -d ecommerce-db)")
        print("  2. Connection: host=localhost, port=5432, user=postgres, password=postgres")
        print("  3. Database: ecommerce")