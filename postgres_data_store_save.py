# store_to_postgres.py

import os
import csv
import psycopg2
from psycopg2.extras import execute_values

class StoreToPostgresUploader:
    def __init__(self, folder='store', table_name='dummy_security_master'):
        # Using 'postgres' for database, user, and password.
        self.dbname = "postgres"
        self.user = "postgres"
        self.password = "postgres"
        self.host = "localhost"
        self.port = 5432
        self.folder = folder
        self.table_name = table_name
        self.conn = psycopg2.connect(
            dbname=self.dbname, 
            user=self.user, 
            password=self.password, 
            host=self.host, 
            port=self.port
        )
    
    def create_table_if_not_exists(self, header):
        """
        Creates the table with columns based on header.
        All columns are created as TEXT.
        """
        columns = ', '.join([f'"{col}" TEXT' for col in header])
        create_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns});"
        with self.conn.cursor() as cur:
            cur.execute(create_query)
            self.conn.commit()
        print(f"Table '{self.table_name}' is ready.")

    def upload_file(self, filepath):
        """
        Reads a CSV file and uploads its rows in bulk to the Postgres table.
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            self.create_table_if_not_exists(header)
            
            rows = []
            with self.conn.cursor() as cur:
                for row in reader:
                    rows.append(tuple(row))
                    if len(rows) >= 1000:  # Batch size of 1000 rows
                        self.bulk_insert(cur, header, rows)
                        rows = []
                if rows:
                    self.bulk_insert(cur, header, rows)
                self.conn.commit()
        print(f"Uploaded file: {filepath}")

    def bulk_insert(self, cur, header, rows):
        """
        Uses psycopg2.extras.execute_values to insert rows in bulk.
        """
        columns = ', '.join([f'"{col}"' for col in header])
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES %s"
        execute_values(cur, query, rows)

    def upload_all_files(self):
        """
        Iterates over all CSV files in the folder and uploads them.
        """
        files = [f for f in os.listdir(self.folder) if f.lower().endswith('.csv')]
        if not files:
            print(f"No CSV files found in folder '{self.folder}'.")
            return
        
        for filename in files:
            filepath = os.path.join(self.folder, filename)
            print(f"Processing file: {filepath}")
            self.upload_file(filepath)
    
    def close(self):
        self.conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    uploader = StoreToPostgresUploader(
        folder="store",
        table_name="dummy_security_master"
    )
    uploader.upload_all_files()
    uploader.close()