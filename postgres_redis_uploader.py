import os
import csv
import psycopg2
from psycopg2.extras import execute_values
import redis
import random
import string
import datetime
import shutil

class PostgresRedisUploader:
    def __init__(self, folder='store', table_name='dummy_security_master'):
        # Postgres connection parameters (all "postgres")
        self.dbname = "postgres"
        self.user = "postgres"
        self.password = "postgres"
        self.host = "localhost"
        self.port = 5432
        
        self.folder = folder
        self.table_name = table_name
        
        # Connect to Postgres
        self.pg_conn = psycopg2.connect(dbname=self.dbname, user=self.user,
                                        password=self.password, host=self.host, port=self.port)
        # Connect to Redis
        self.redis_conn = redis.Redis(host='localhost', port=6379, db=0)
        # Use a prefix for our redis keys
        self.index_fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]

    def create_table_if_not_exists(self, header):
        """
        Creates a Postgres table with columns based on the CSV header.
        All columns are created as TEXT.
        """
        columns = ', '.join([f'"{col}" TEXT' for col in header])
        create_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns});"
        with self.pg_conn.cursor() as cur:
            cur.execute(create_query)
            self.pg_conn.commit()
        print(f"Table '{self.table_name}' is ready.")

    def bulk_insert_postgres(self, header, rows, batch_size=1000):
        """
        Bulk inserts rows into Postgres table using execute_values.
        """
        with self.pg_conn.cursor() as cur:
            query = f"INSERT INTO {self.table_name} ({', '.join([f'\"{col}\"' for col in header])}) VALUES %s"
            batch = []
            for row in rows:
                batch.append(tuple(row))
                if len(batch) >= batch_size:
                    execute_values(cur, query, batch)
                    batch = []
            if batch:
                execute_values(cur, query, batch)
            self.pg_conn.commit()
        print(f"Inserted {len(rows)} rows into Postgres.")

    def _make_composite_key(self, record):
        """
        Creates a composite key from the first 8 columns of the record.
        For example:
          security:BBG000BLNNH6|037833100|2046251|US0378331005|Apple Inc.|USD|Equity|Domestic Equity
        """
        key_parts = [record.get(field, "").strip() for field in self.index_fields]
        return "security:" + "|".join(key_parts)

    def update_redis_cache(self, record):
        """
        Updates Redis by storing the record in a hash keyed by its composite key.
        """
        key = self._make_composite_key(record)
        self.redis_conn.hset(key, mapping=record)
        # Optionally, update additional index sets here if needed.
        print(f"Updated Redis for key: {key}")

    def process_file(self, filepath):
        """
        For a given CSV file:
         - Create the Postgres table (if needed).
         - Bulk insert all rows into Postgres.
         - For each row, update the Redis cache.
        """
        print(f"Processing file: {filepath}")
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
        
        # Create Postgres table if it doesn't exist
        self.create_table_if_not_exists(header)
        # Bulk insert into Postgres
        self.bulk_insert_postgres(header, rows)
        
        # Update Redis for each row.
        # Convert each row into a dictionary using the header.
        for row in rows:
            record = dict(zip(header, row))
            self.update_redis_cache(record)
        print(f"Finished processing file: {filepath}")

    def process_all_files(self):
        """
        Iterates over all CSV files in the folder and processes them.
        """
        files = [f for f in os.listdir(self.folder) if f.lower().endswith('.csv')]
        if not files:
            print(f"No CSV files found in folder '{self.folder}'.")
            return
        for filename in files:
            filepath = os.path.join(self.folder, filename)
            self.process_file(filepath)

    def close(self):
        self.pg_conn.close()
        self.redis_conn.close()
        print("Closed Postgres and Redis connections.")

if __name__ == "__main__":
    # Using defaults: folder "store", table name "dummy_security_master".
    uploader = PostgresRedisUploader(folder="store", table_name="dummy_security_master")
    uploader.process_all_files()
    uploader.close()