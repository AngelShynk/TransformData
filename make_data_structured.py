import duckdb
import pandas as pd

# Load CSV into a pandas DataFrame
csv_file = 'sales_data_with_json.csv'
df = pd.read_csv(csv_file)

# Connect to DuckDB (in-memory database or file-based if you prefer)
conn = duckdb.connect('sales.duckdb')

# Create a temporary table and insert the CSV data
conn.execute("CREATE TABLE IF NOT EXISTS sales_raw AS SELECT * FROM df")

# Transform JSON columns into structured columns
query = """
    CREATE TABLE IF NOT EXISTS sales_structured AS
    SELECT
        sales_id,
        -- Extract product details from the JSON column
        json_extract(product_info, '$.product_id')::INT AS product_id,
        json_extract(product_info, '$.product_name')::TEXT AS product_name,
        json_extract(product_info, '$.product_category')::TEXT AS product_category,
        -- Extract store details from the JSON column
        json_extract(store_info, '$.store_id')::INT AS store_id,
        json_extract(store_info, '$.store_name')::TEXT AS store_name,
        json_extract(store_info, '$.store_location')::TEXT AS store_location,
        customer_id,
        sales_date,
        total_sales,
        total_quantity_sold
    FROM sales_raw
"""

# Execute the transformation query
conn.execute(query)

# Set pandas options to display all columns
pd.set_option('display.max_columns', None)


# Verify that the data is saved by querying both tables
print("Raw Data Table:")
print(conn.execute("SELECT * FROM sales_raw LIMIT 5").fetchdf())

print("\nStructured Data Table:")
print(conn.execute("SELECT * FROM sales_structured LIMIT 5").fetchdf())

# Close the connection when done
conn.close()
