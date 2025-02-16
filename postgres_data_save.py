import csv
import psycopg2
from psycopg2.extras import execute_values

class PostgresUploader:
    def __init__(self, dbname, user, password, host='localhost', port=5432, table_name='dummy_security_master'):
        self.connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.table_name = table_name

    def create_table(self, csv_file):
        """
        Reads the CSV header and creates a table with all columns as TEXT.
        """
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
        
        # Build a CREATE TABLE query where each column is type TEXT.
        columns = ', '.join([f'"{col}" TEXT' for col in header])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns});"
        
        with self.connection.cursor() as cur:
            cur.execute(create_table_query)
            self.connection.commit()
        print(f"Table '{self.table_name}' created (or already exists).")

    def upload_csv(self, csv_file, batch_size=1000):
        """
        Reads the CSV file and uploads its rows in batches to the PostgreSQL table.
        """
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Extract header row
            rows = []
            with self.connection.cursor() as cur:
                for row in reader:
                    rows.append(row)
                    if len(rows) >= batch_size:
                        self.bulk_insert(cur, header, rows)
                        rows = []
                # Insert any remaining rows
                if rows:
                    self.bulk_insert(cur, header, rows)
                self.connection.commit()
        print(f"CSV file '{csv_file}' uploaded to table '{self.table_name}'.")

    def bulk_insert(self, cur, header, rows):
        """
        Uses psycopg2.extras.execute_values for bulk insertion.
        """
        # Build an INSERT query using the header columns
        columns = ', '.join([f'"{col}"' for col in header])
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES %s"
        tuples = [tuple(row) for row in rows]
        execute_values(cur, query, tuples)

    def close(self):
        """Closes the PostgreSQL connection."""
        self.connection.close()

# Example usage:
if __name__ == "__main__":
    # Replace these values with your actual PostgreSQL credentials.
    uploader = PostgresUploader(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432,
        table_name="dummy_security_master"
    )
    
    csv_file = "dummy_security_master.csv"
    
    # Create table based on CSV header and then upload CSV data.
    uploader.create_table(csv_file)
    uploader.upload_csv(csv_file)
    uploader.close()