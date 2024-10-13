import pandas as pd
import json

# Sample dataset with 5 rows
data = {
    'sales_id': [1, 2, 3, 4, 5],
    'product_info': [
        json.dumps({"product_id": 101, "product_name": "Laptop", "product_category": "Electronics"}),
        json.dumps({"product_id": 102, "product_name": "Smartphone", "product_category": "Electronics"}),
        json.dumps({"product_id": 103, "product_name": "TV", "product_category": "Electronics"}),
        json.dumps({"product_id": 101, "product_name": "Laptop", "product_category": "Electronics"}),
        json.dumps({"product_id": 102, "product_name": "Smartphone", "product_category": "Electronics"})
    ],
    'store_info': [
        json.dumps({"store_id": 1, "store_name": "Store A", "store_location": "New York"}),
        json.dumps({"store_id": 1, "store_name": "Store A", "store_location": "New York"}),
        json.dumps({"store_id": 2, "store_name": "Store B", "store_location": "Chicago"}),
        json.dumps({"store_id": 2, "store_name": "Store B", "store_location": "Chicago"}),
        json.dumps({"store_id": 3, "store_name": "Store C", "store_location": "San Francisco"})
    ],
    'customer_id': [201, 202, 203, 204, 205],
    'sales_date': ["2023-10-01", "2023-10-01", "2023-10-02", "2023-10-02", "2023-10-03"],
    'total_sales': [10000.00, 7500.00, 15000.00, 5000.00, 12000.00],
    'total_quantity_sold': [10, 15, 10, 5, 24]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save to CSV
file_path = 'sales_data_with_json.csv'
df.to_csv(file_path, index=False)

df.head(), file_path
