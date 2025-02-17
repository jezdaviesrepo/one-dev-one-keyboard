import csv
import glob
import os
import datetime
import random
import psycopg2
import redis
import time
from os import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed

def clear_redis_keys():
    """Clears all keys in Redis (and the security_keys index)."""
    r = redis.Redis(host="localhost", port=6379, db=0)
    r.flushdb()
    print("Redis keys cleared.")

def drop_and_create_postgres_table(model_columns, table_name="security_master"):
    """
    Drops the Postgres table if it exists and creates a new table with all columns as TEXT.
    model_columns: list of column names.
    """
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
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
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
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

def load_inventory_to_redis(inventory_dir):
    """
    Iterates over all CSV files in the inventory directory and loads each row into Redis as a hash.
    Constructs the Redis key by concatenating the values of the first 8 model columns.
    Also, for each record, adds the key to the sorted set 'security_keys' (using current time as score) for efficient pagination.
    """
    r = redis.Redis(host="localhost", port=6379, db=0)
    files = glob.glob(os.path.join(inventory_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in directory '{inventory_dir}'.")
        return

    count = 0
    # Define the fixed field names (first 8 model columns)
    key_fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    for filepath in files:
        with open(filepath, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                key_values = [row.get(field, "").strip() for field in key_fields]
                if any(v == "" for v in key_values):
                    key = f"record:{os.path.basename(filepath)}:{random.randint(100000,999999)}"
                else:
                    key = "|".join(key_values)
                r.hset(key, mapping=row)
                # Add the key to a sorted set with current time as score for pagination.
                r.zadd("security_keys", {key: time.time()})
                count += 1
    print(f"Loaded {count} records into Redis.")

def main():
    inventory_dir = "inventory"  # Directory containing CSV files.
    
    clear_redis_keys()
    
    files = glob.glob(os.path.join(inventory_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in '{inventory_dir}'. Exiting.")
        return
    with open(files[0], newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        model_columns = reader.fieldnames
    print("Model columns detected:", model_columns)
    
    drop_and_create_postgres_table(model_columns, table_name="security_master")
    populate_postgres_table(inventory_dir, table_name="security_master")
    load_inventory_to_redis(inventory_dir)

if __name__ == "__main__":
    main()