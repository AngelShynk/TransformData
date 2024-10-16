import duckdb
import csv
import json
from datetime import datetime
from typing import Any, List, Dict


# Define Kafka-like metadata generator
def generate_kafka_metadata() -> Dict[str, Any]:
    return {
        "event_id": "evt_" + datetime.utcnow().strftime("%Y%m%d%H%M%S"),
        "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "partition": 0,
        "offset": 12345,
        "source": "source_system"
    }


# Define raw nested JSON with null variants
def generate_raw_json_data() -> List[Dict[str, Any]]:
    return [
        {
            "user": {"id": 1, "name": "Alice", "email": None},
            "action": "login",
            "details": {"location": "NY", "device": "mobile", "session": "null"}
        },
        {
            "user": {"id": 2, "name": "Bob", "email": "null"},
            "action": "purchase",
            "details": {"location": "LA", "device": None, "session": "abc123"}
        },
        {
            "user": {"id": 3, "name": "null", "email": "bob@example.com"},
            "action": "logout",
            "details": {"location": None, "device": "desktop", "session": "-"}
        },
        # Add more entries up to 10
    ]


# Write data to CSV file
def write_to_csv(filename: str):
    metadata = generate_kafka_metadata()
    raw_data = generate_raw_json_data()

    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["metadata", "raw_data"])
        writer.writeheader()

        for record in raw_data:
            writer.writerow({
                "metadata": json.dumps(metadata),
                "raw_data": json.dumps(record)
            })


# Load CSV into DuckDB
def load_into_duckdb(csv_file: str, db_file: str):
    conn = duckdb.connect(db_file)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS raw_events (
            metadata JSON,
            raw_data JSON
        )
    """)
    conn.execute(f"""
        COPY raw_events FROM '{csv_file}' (AUTO_DETECT TRUE);
    """)
    conn.close()


# Query raw data, parse JSON and write to staging and mart tables
def process_data_in_duckdb(db_file: str):
    conn = duckdb.connect(db_file)

    # Stage table creation with JSON parsing and null cleaning
    conn.execute("""
            CREATE OR REPLACE TABLE stage_events AS
            WITH prepared_cte AS (
                SELECT 
                    json_extract_string(metadata, '$.event_id') AS event_id,  -- String value
                    CAST(json_extract_string(metadata, '$.event_timestamp') AS TIMESTAMP) AS event_timestamp,  -- Timestamp type
                    CAST(json_extract(metadata, '$.partition') AS INTEGER) AS partition_,  -- Integer value
                    CAST(json_extract(metadata, '$.offset') AS BIGINT) AS offset_,  -- BigInt for larger numeric values
                    json_extract_string(metadata, '$.source') AS source_,  -- String value
                    CAST(json_extract(raw_data, '$.user.id') AS INTEGER) AS user_id,  -- Integer value
                    json_extract_string(raw_data, '$.user.name') AS user_name,
                    json_extract_string(raw_data, '$.user.email') AS user_email,
                    json_extract_string(raw_data, '$.action') AS action_,  -- Action as string
                    json_extract_string(raw_data, '$.details.location') AS location,
                    json_extract_string(raw_data, '$.details.device') AS device,
                    json_extract_string(raw_data, '$.details.session') AS session_
                FROM raw_events
            )
            
            SELECT 
                event_id,
                event_timestamp,
                partition_,
                offset_,
                source_,
                user_id,
                user_name,
                IF(user_email = 'null', NULL, user_email) AS user_email,
                action_,
                location,
                device,
                IF(session_ IN ('null', '-'), NULL, session_ ) as session
            FROM prepared_cte
            ;
        """)

    # Deduplication using QUALIFY
    conn.execute("""
        CREATE OR REPLACE TABLE deduplicated_stage_events AS
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp DESC) AS row_num
        FROM stage_events
        QUALIFY row_num = 1;
    """)

    # Aggregated data into mart table
    conn.execute("""
        CREATE OR REPLACE TABLE mart_aggregated_data AS
        SELECT 
            user_id,
            COUNT(*) AS total_actions,
            COUNT(DISTINCT action_) AS unique_actions,
            MAX(event_timestamp) AS last_event_timestamp
        FROM deduplicated_stage_events
        GROUP BY user_id;
    """)

    conn.close()


# Run the steps
csv_filename = "kafka_events.csv"
duckdb_file = "sales.duckdb"

# write_to_csv(csv_filename)  # Step 1: Write to CSV
load_into_duckdb(csv_filename, duckdb_file)  # Step 2: Load into DuckDB
process_data_in_duckdb(duckdb_file)  # Step 3: Process data
