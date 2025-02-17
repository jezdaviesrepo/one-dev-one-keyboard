import csv
import random
import string
import datetime
import os

def add_business_day(date_obj):
    """Adds one business day to date_obj (skipping weekends)."""
    next_day = date_obj + datetime.timedelta(days=1)
    while next_day.weekday() >= 5:  # 5=Saturday, 6=Sunday
        next_day += datetime.timedelta(days=1)
    return next_day

class SecurityMasterDailyUpdaterVendor:
    def __init__(self, input_filename, vendor_name=None):
        self.input_filename = input_filename
        self.vendor_name = vendor_name  # If None, will be extracted from filename.
        self.fieldnames = []
        self.data = []
        self.current_date = None

    def extract_vendor_name(self):
        base = os.path.basename(self.input_filename)
        if "_" in base:
            self.vendor_name = base.split("_")[0]
        else:
            self.vendor_name = os.path.splitext(base)[0]

    def read_file(self):
        """Reads the CSV file (with underscore-prefixed headers for fixed and dummy fields, but APPLIED_DATE is plain)
           and stores its rows.
        """
        with open(self.input_filename, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            self.fieldnames = reader.fieldnames  # e.g. ["_FIGI", "_CUSIP", ... , "APPLIED_DATE"]
            self.data = list(reader)
        if not self.vendor_name:
            self.extract_vendor_name()
        # Determine starting simulated date from the "APPLIED_DATE" field.
        dates = []
        for row in self.data:
            d_str = row.get("APPLIED_DATE", "").strip()
            if d_str:
                try:
                    dates.append(datetime.datetime.strptime(d_str, "%Y-%m-%d").date())
                except Exception:
                    pass
        self.current_date = max(dates) if dates else datetime.date.today()
        print(f"Loaded {len(self.data)} rows from {self.input_filename}. Starting simulated date: {self.current_date.isoformat()}")

    def detect_type(self, value):
        """Heuristically determines the type of a string value."""
        if value.strip() == "":
            return None
        try:
            if '.' not in value:
                int(value)
                return "integer"
        except ValueError:
            pass
        try:
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
        """Generates a new dummy value (as string) based on the detected type."""
        if typ == "integer":
            return str(random.randint(0, 100))
        elif typ == "float":
            return str(round(random.uniform(0, 100), 2))
        elif typ == "date":
            start_date = datetime.date(2000, 1, 1)
            end_date = datetime.date(2025, 12, 31)
            delta = (end_date - start_date).days
            return (start_date + datetime.timedelta(days=random.randint(0, delta))).isoformat()
        else:
            return ''.join(random.choices(string.ascii_letters, k=10))

    def modify_rows_for_day(self, num_rows_to_modify, num_fields_to_change):
        total_rows = len(self.data)
        if num_rows_to_modify > total_rows:
            num_rows_to_modify = total_rows

        # Assume fixed fields: first 8 underscore-prefixed fields (e.g. _FIGI, _CUSIP, ... _ASSET_GROUP)
        # The last field is "APPLIED_DATE" (without underscores), and dummy fields are those between index 8 and the last.
        modifiable_fields = self.fieldnames[8:-1]
        selected_indices = random.sample(range(total_rows), num_rows_to_modify)

        for idx in selected_indices:
            row_modified = False
            if num_fields_to_change > len(modifiable_fields):
                fields_to_change = modifiable_fields
            else:
                fields_to_change = random.sample(modifiable_fields, num_fields_to_change)
            for field in fields_to_change:
                original_value = self.data[idx][field]
                if original_value.strip() == "":
                    continue
                typ = self.detect_type(original_value)
                if typ is None:
                    continue
                new_value = self.generate_dummy_value_by_type(typ)
                if new_value != original_value:
                    self.data[idx][field] = new_value
                    row_modified = True
            if row_modified:
                # Read the current business date from the "APPLIED_DATE" field (which is not underscored).
                applied_str = self.data[idx].get("APPLIED_DATE", "").strip()
                if applied_str:
                    try:
                        current_date = datetime.datetime.strptime(applied_str, "%Y-%m-%d").date()
                    except Exception:
                        current_date = self.current_date
                    new_date = add_business_day(current_date)
                    self.data[idx]["APPLIED_DATE"] = new_date.isoformat()

    def run_for_days(self, num_days, num_rows_to_modify, num_fields_to_change):
        """Simulates modifications over a specified number of days.
           After each day, increments the simulated date by one business day and writes a new CSV
           file to the "store" directory with a filename formatted as:
           <vendor_name>_<yyyy-mm-dd>.csv.
        """
        store_dir = "store"
        os.makedirs(store_dir, exist_ok=True)
        for day in range(num_days):
            print(f"Simulating day {day + 1}...")
            self.modify_rows_for_day(num_rows_to_modify, num_fields_to_change)
            self.current_date = add_business_day(self.current_date)
            output_filename = os.path.join(store_dir, f"{self.vendor_name}_{self.current_date.isoformat()}.csv")
            self.save_file(output_filename)

    def save_file(self, output_filename):
        with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in self.data:
                writer.writerow(row)
        print(f"Saved file: {output_filename}")

if __name__ == "__main__":
    input_file = input("Enter input CSV filename: ")
    try:
        num_days = int(input("Enter number of days to simulate modifications: "))
        num_rows_to_modify = int(input("Enter number of rows to modify per day: "))
        num_fields_to_change = int(input("Enter number of fields to change per modified row: "))
    except ValueError:
        print("Invalid input.")
        exit(1)
    
    updater = SecurityMasterDailyUpdaterVendor(input_file)
    updater.read_file()
    updater.run_for_days(num_days, num_rows_to_modify, num_fields_to_change)