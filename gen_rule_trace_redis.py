import csv
import glob
import os
import datetime
import random
import psycopg2
import psycopg2.extras
import redis
import time
import json
from os import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Redis Operations ----------

def clear_redis_keys():
    """Clears all keys in Redis."""
    r = redis.Redis(host="localhost", port=6379, db=0)
    r.flushdb()
    print("Redis keys cleared.")

# ---------- Postgres Operations ----------

def drop_and_create_postgres_table(model_columns, table_name="security_master"):
    """
    Drops the Postgres table if it exists and creates a new table with all columns as TEXT.
    model_columns: list of column names.
    """
    conn = psycopg2.connect(dbname="postgres", user="jez", password="", host="localhost", port=5432)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    print(f"Postgres table '{table_name}' dropped.")
    
    columns_sql = ", ".join([f'"{col}" TEXT' for col in model_columns])
    create_sql = f"CREATE TABLE {table_name} ({columns_sql});"
    cur.execute(create_sql)
    print(f"Postgres table '{table_name}' created with columns: {model_columns}")
    
    cur.close()
    conn.close()

def populate_postgres_table_for_file(filepath, table_name="security_master", batch_size=100):
    """
    Processes one CSV file from the inventory directory and inserts its rows into the Postgres table.
    Uses psycopg2.extras.execute_batch to insert rows in batches.
    """
    conn = psycopg2.connect(dbname="postgres", user="jez", password="", host="localhost", port=5432)
    conn.autocommit = True
    cur = conn.cursor()
    
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        rows = list(reader)
    
    if not rows:
        print(f"No rows found in {filepath}")
        cur.close()
        conn.close()
        return 0

    columns = [f'"{col}"' for col in headers]
    columns_sql = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(headers))
    insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders});"
    
    # Create a list of value tuples
    values_list = [[row[col] for col in headers] for row in rows]
    
    # Use execute_batch for better performance
    psycopg2.extras.execute_batch(cur, insert_sql, values_list, page_size=batch_size)
    count = len(values_list)
    
    print(f"Inserted {count} records from {os.path.basename(filepath)} into Postgres table '{table_name}'.")
    
    cur.close()
    conn.close()
    return count

def populate_postgres_table(inventory_dir, table_name="security_master"):
    """
    Iterates over all CSV files in the inventory directory and inserts their rows into the Postgres table
    concurrently using a thread pool.
    """
    files = glob.glob(os.path.join(inventory_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in directory '{inventory_dir}'.")
        return
    
    total_inserted = 0
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        futures = [executor.submit(populate_postgres_table_for_file, filepath, table_name) for filepath in files]
        for future in as_completed(futures):
            total_inserted += future.result()
    print(f"Total inserted records into Postgres table '{table_name}': {total_inserted}")

# ---------- CSV Loader to Redis ----------

def load_inventory_file_to_redis(filepath, key_fields):
    """
    Loads one CSV file from the inventory directory into Redis.
    For each row, constructs a key using the first 8 columns (key_fields).
    Uses a Redis pipeline for better performance.
    """
    r = redis.Redis(host="localhost", port=6379, db=0)
    count = 0
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        pipe = r.pipeline()
        for row in reader:
            key_values = [row.get(field, "").strip() for field in key_fields]
            if any(v == "" for v in key_values):
                key = f"record:{os.path.basename(filepath)}:{random.randint(100000,999999)}"
            else:
                key = "|".join(key_values)
            # Store the entire row as a hash
            pipe.hset(key, mapping=row)
            count += 1
            # Execute in batches of 100 commands
            if count % 100 == 0:
                pipe.execute()
        pipe.execute()
    print(f"Loaded {count} records from {os.path.basename(filepath)} into Redis.")
    return count

def load_inventory_to_redis(inventory_dir):
    """
    Iterates over all CSV files in the inventory directory and loads each row into Redis concurrently.
    """
    r = redis.Redis(host="localhost", port=6379, db=0)
    files = glob.glob(os.path.join(inventory_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in directory '{inventory_dir}'.")
        return

    key_fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    total_loaded = 0
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        futures = [executor.submit(load_inventory_file_to_redis, filepath, key_fields) for filepath in files]
        for future in as_completed(futures):
            total_loaded += future.result()
    print(f"Total loaded records into Redis: {total_loaded}")

# ---------- New: Load Rule Trace to Redis ----------

def load_rule_trace_to_redis(rule_trace_dir):
    """
    Iterates over all CSV files in the rule_trace directory.
    For each CSV file, reads each row (error/warning record) and pushes it as a JSON string
    into a Redis list with key "rule_trace".
    """
    r = redis.Redis(host="localhost", port=6379, db=0)
    files = glob.glob(os.path.join(rule_trace_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in directory '{rule_trace_dir}'.")
        return
    total = 0
    pipe = r.pipeline()
    for filepath in files:
        with open(filepath, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Each row should have a UniqueKey field (from the rule engine report)
                unique_key = row.get("UniqueKey", "")
                # We push the entire record as JSON into a list stored under key "rule_trace"
                pipe.rpush("rule_trace", json.dumps(row))
                total += 1
    pipe.execute()
    print(f"Loaded {total} rule trace records into Redis under key 'rule_trace'.")

# ---------- Main Function ----------

def main():
    inventory_dir = "inventory"  # Directory containing model CSV files.
    
    # Step 1: Clear Redis keys.
    clear_redis_keys()
    
    # Step 2: Process the first CSV in the inventory directory to obtain model columns.
    files = glob.glob(os.path.join(inventory_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in '{inventory_dir}'. Exiting.")
        return
    with open(files[0], newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        model_columns = reader.fieldnames
    print("Model columns detected:", model_columns)
    
    # Step 3: Drop and create the Postgres table.
    drop_and_create_postgres_table(model_columns, table_name="security_master")
    
    # Step 4: Populate the Postgres table with data from all CSV files in inventory concurrently.
    populate_postgres_table(inventory_dir, table_name="security_master")
    
    # Step 5: Load inventory files into Redis concurrently.
    load_inventory_to_redis(inventory_dir)
    
    # New Step 6: Load rule trace files from the rule_trace directory into Redis.
    rule_trace_dir = "rule_trace"
    load_rule_trace_to_redis(rule_trace_dir)

if __name__ == "__main__":
    main()