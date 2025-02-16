import os
import csv
import random
import string
import datetime
import shutil

class SecurityMasterDailyUpdater:
    def __init__(self, input_filename, num_days, num_rows_to_modify, num_fields_to_change):
        self.input_filename = input_filename
        self.num_days = num_days
        self.num_rows_to_modify = num_rows_to_modify
        self.num_fields_to_change = num_fields_to_change
        self.fieldnames = []
        self.data = []
        self.load_file()
        self.store_folder = "store"
        if not os.path.exists(self.store_folder):
            os.makedirs(self.store_folder)
        # Initialize current_simulated_date from the max APPLIED_DATE in the file or today.
        applied_dates = [row["APPLIED_DATE"] for row in self.data if row["APPLIED_DATE"].strip()]
        if applied_dates:
            self.current_simulated_date = max(datetime.datetime.strptime(d, "%Y-%m-%d").date() for d in applied_dates)
        else:
            self.current_simulated_date = datetime.date.today()

    def load_file(self):
        """Load the CSV file into self.data and store fieldnames."""
        with open(self.input_filename, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            self.fieldnames = reader.fieldnames
            self.data = list(reader)
        print(f"Loaded {len(self.data)} rows with {len(self.fieldnames)} columns.")

    def save_original_file(self):
        """Copies the original input file to the store folder using its original filename."""
        original_filename = os.path.basename(self.input_filename)
        target_path = os.path.join(self.store_folder, original_filename)
        shutil.copy2(self.input_filename, target_path)
        print(f"Copied original file to {target_path}")

    def detect_type(self, value):
        """
        Detect the type of the given value.
        Returns "integer", "float", "date", or "string". Returns None for empty values.
        """
        if value.strip() == "":
            return None
        try:
            if '.' not in value:
                int(value)
                return "integer"
        except ValueError:
            pass
        try:
            if '.' in value:
                float(value)
                return "float"
        except ValueError:
            pass
        try:
            datetime.datetime.strptime(value, "%Y-%m-%d")
            return "date"
        except ValueError:
            pass
        return "string"

    def generate_dummy_value_by_type(self, typ):
        """Generate a dummy value (as string) based on the given type."""
        if typ == "integer":
            return str(random.randint(0, 100))
        elif typ == "float":
            return str(round(random.uniform(0, 100), 2))
        elif typ == "date":
            start_date = datetime.date(2000, 1, 1)
            end_date = datetime.date(2025, 12, 31)
            delta_days = (end_date - start_date).days
            random_days = random.randint(0, delta_days)
            return (start_date + datetime.timedelta(days=random_days)).isoformat()
        else:  # "string"
            return ''.join(random.choices(string.ascii_letters, k=10))

    def add_business_day(self, date_obj):
        """Adds one business day to date_obj (skipping weekends)."""
        next_day = date_obj + datetime.timedelta(days=1)
        while next_day.weekday() >= 5:  # Saturday=5, Sunday=6
            next_day += datetime.timedelta(days=1)
        return next_day

    def modify_rows_for_day(self):
        """
        Randomly selects rows and modifies a given number of fields (columns 9 to the one before APPLIED_DATE).
        For any row modified, update its APPLIED_DATE by adding one business day.
        Returns the number of rows modified.
        """
        total_rows = len(self.data)
        if self.num_rows_to_modify > total_rows:
            rows_indices = list(range(total_rows))
        else:
            rows_indices = random.sample(range(total_rows), self.num_rows_to_modify)
        # Modifiable fields: columns 9 to the one before the last column.
        modifiable_fields = self.fieldnames[8:-1]
        modified_count = 0
        for idx in rows_indices:
            row_modified = False
            if self.num_fields_to_change > len(modifiable_fields):
                fields_to_change = modifiable_fields
            else:
                fields_to_change = random.sample(modifiable_fields, self.num_fields_to_change)
            for field in fields_to_change:
                original_value = self.data[idx][field]
                if original_value.strip() == "":
                    continue  # Leave originally empty fields unchanged.
                typ = self.detect_type(original_value)
                if typ is None:
                    continue
                new_value = self.generate_dummy_value_by_type(typ)
                if new_value != original_value:
                    self.data[idx][field] = new_value
                    row_modified = True
            if row_modified:
                modified_count += 1
                applied_date_str = self.data[idx].get("APPLIED_DATE", "").strip()
                if applied_date_str:
                    try:
                        current_date = datetime.datetime.strptime(applied_date_str, "%Y-%m-%d").date()
                        new_date = self.add_business_day(current_date)
                        self.data[idx]["APPLIED_DATE"] = new_date.isoformat()
                    except Exception as e:
                        print(f"Error updating APPLIED_DATE for row {idx}: {e}")
        return modified_count

    def get_representative_date(self):
        """
        Determines a representative date for the file by taking the maximum APPLIED_DATE among all rows.
        """
        applied_dates = []
        for row in self.data:
            date_str = row.get("APPLIED_DATE", "").strip()
            if date_str:
                try:
                    d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                    applied_dates.append(d)
                except Exception as e:
                    print(f"Error parsing APPLIED_DATE: {e}")
        if applied_dates:
            return max(applied_dates)
        return datetime.date.today()

    def save_day_file(self, rep_date):
        """
        Saves the current state of self.data to a file in the 'store' folder.
        The filename is 'response_<yyyy-mm-dd>.csv', where <yyyy-mm-dd> is rep_date.
        """
        filename = f"response_{rep_date.isoformat()}.csv"
        filepath = os.path.join(self.store_folder, filename)
        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in self.data:
                writer.writerow(row)
        print(f"Saved file: {filepath}")

    def run(self):
        """
        Simulate modifications over a set number of days.
        First, copy the original file to the store folder (with its original filename).
        Then, for each day:
          - Modify rows.
          - Determine a representative date (max APPLIED_DATE among all rows).
          - Save the file with that date in the filename.
        """
        # Save the original file without renaming it.
        self.save_original_file()
        for day in range(self.num_days):
            print(f"--- Processing day {day+1} ---")
            modified_rows = self.modify_rows_for_day()
            rep_date = self.get_representative_date()
            self.save_day_file(rep_date)
            print(f"Day {day+1}: Modified {modified_rows} rows. File saved with date {rep_date}.")

# Usage example:
if __name__ == "__main__":
    input_file = input("Enter the input CSV filename: ")
    num_days = int(input("Enter the number of business days to simulate: "))
    num_rows_to_modify = int(input("Enter the number of rows to modify each day: "))
    num_fields_to_change = int(input("Enter the number of fields to change per modified row: "))
    
    updater = SecurityMasterDailyUpdater(input_file, num_days, num_rows_to_modify, num_fields_to_change)
    updater.run()