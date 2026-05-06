import pandas as pd
from datetime import datetime
from pathlib import Path
from etl.extractors.api_extractor import extract_users, extract_products
from etl.extractors.db_extractor import extract_orders
from etl.transformers.transformer import transform_users, transform_products, transform_orders

# Directory for export files
output_dir = Path("etl_output")
output_dir.mkdir(exist_ok=True)
export_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Set pandas options to display all columns and rows
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

# Extract from DummyJSON endpoints
print("Extracting users from DummyJSON...")
df_users = extract_users()
print(f"Raw users extracted: {len(df_users)} rows")

print("Extracting products from DummyJSON...")
df_products = extract_products()
print(f"Raw products extracted: {len(df_products)} rows")

print("Extracting orders from PostgreSQL...")
df_orders = extract_orders()
print(f"Raw orders extracted: {len(df_orders)} rows")

# Transform data
transformed_users = transform_users(df_users)
print(f"Transformed users: {len(transformed_users)} rows")

transformed_products = transform_products(df_products)
print(f"Transformed products: {len(transformed_products)} rows")

transformed_orders = transform_orders(df_orders)
print(f"Transformed orders: {len(transformed_orders)} rows")

# Export data to files
text_file = output_dir / f"etl_output_{export_timestamp}.txt"
with text_file.open("w", encoding="utf-8") as f:
    f.write("Raw Users DataFrame:\n")
    f.write(df_users.to_string(index=False))
    f.write("\n\n" + "="*50 + "\n\n")
    f.write("Transformed Users DataFrame:\n")
    f.write(transformed_users.to_string(index=False))
    f.write("\n\n" + "="*50 + "\n\n")
    f.write("Raw Products DataFrame:\n")
    f.write(df_products.to_string(index=False))
    f.write("\n\n" + "="*50 + "\n\n")
    f.write("Transformed Products DataFrame:\n")
    f.write(transformed_products.to_string(index=False))
    f.write("\n\n" + "="*50 + "\n\n")
    f.write("Raw Orders DataFrame:\n")
    f.write(df_orders.head(10).to_string(index=False))  # Show first 10 orders
    f.write("\n\n" + "="*50 + "\n\n")
    f.write("Transformed Orders DataFrame:\n")
    f.write(transformed_orders.head(10).to_string(index=False))  # Show first 10 transformed orders

# Export CSV files for easy review
users_raw_csv = output_dir / f"users_raw_{export_timestamp}.csv"
df_users.to_csv(users_raw_csv, index=False)
users_transformed_csv = output_dir / f"users_transformed_{export_timestamp}.csv"
transformed_users.to_csv(users_transformed_csv, index=False)

products_raw_csv = output_dir / f"products_raw_{export_timestamp}.csv"
df_products.to_csv(products_raw_csv, index=False)
products_transformed_csv = output_dir / f"products_transformed_{export_timestamp}.csv"
transformed_products.to_csv(products_transformed_csv, index=False)

orders_raw_csv = output_dir / f"orders_raw_{export_timestamp}.csv"
df_orders.to_csv(orders_raw_csv, index=False)
orders_transformed_csv = output_dir / f"orders_transformed_{export_timestamp}.csv"
transformed_orders.to_csv(orders_transformed_csv, index=False)

print("\nExported full output to:")
print(f"- {text_file}")
print(f"- {users_raw_csv}")
print(f"- {users_transformed_csv}")
print(f"- {products_raw_csv}")
print(f"- {products_transformed_csv}")
print(f"- {orders_raw_csv}")
print(f"- {orders_transformed_csv}")