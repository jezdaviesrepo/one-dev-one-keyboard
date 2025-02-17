import csv
import glob
import os
import datetime
import random
import psycopg2
import redis

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

def populate_postgres_table(inventory_dir, table_name="security_master"):
    """
    Iterates over all CSV files in the inventory directory and inserts their rows into the Postgres table.
    """
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
    conn.autocommit = True
    cur = conn.cursor()
    
    files = glob.glob(os.path.join(inventory_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in directory '{inventory_dir}'.")
        cur.close()
        conn.close()
        return
    
    total_inserted = 0
    for filepath in files:
        with open(filepath, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames
            rows = list(reader)
            columns = ', '.join([f'"{col}"' for col in headers])
            placeholders = ', '.join(['%s'] * len(headers))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            for row in rows:
                values = [row[col] for col in headers]
                cur.execute(insert_sql, values)
                total_inserted += 1
    print(f"Inserted {total_inserted} records into Postgres table '{table_name}'.")
    cur.close()
    conn.close()

# ---------- CSV Loader to Redis ----------

def load_inventory_to_redis(inventory_dir):
    """
    Iterates over all CSV files in the inventory directory and loads each row into Redis as a hash.
    Constructs the Redis key by concatenating the values of the first 8 columns (lower snake case) separated by a pipe.
    """
    r = redis.Redis(host="localhost", port=6379, db=0)
    files = glob.glob(os.path.join(inventory_dir, "*.csv"))
    if not files:
        print(f"No CSV files found in directory '{inventory_dir}'.")
        return

    count = 0
    # Define the fixed field names (first 8 model columns).
    key_fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    for filepath in files:
        with open(filepath, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                key_values = [row.get(field, "").strip() for field in key_fields]
                # If any key field is empty, generate a fallback key.
                if any(v == "" for v in key_values):
                    key = f"record:{os.path.basename(filepath)}:{random.randint(100000,999999)}"
                else:
                    key = "|".join(key_values)
                r.hset(key, mapping=row)
                count += 1
    print(f"Loaded {count} records into Redis.")

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
    
    # Step 4: Populate the Postgres table with data from all CSV files in inventory.
    populate_postgres_table(inventory_dir, table_name="security_master")
    
    # Step 5: Load inventory files into Redis.
    load_inventory_to_redis(inventory_dir)

if __name__ == "__main__":
    main()