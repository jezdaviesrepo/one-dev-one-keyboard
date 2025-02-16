import csv
import random
import string
import datetime

def add_business_day(date_obj):
    """Adds one business day to date_obj (skipping weekends)."""
    next_day = date_obj + datetime.timedelta(days=1)
    while next_day.weekday() >= 5:  # Saturday=5, Sunday=6
        next_day += datetime.timedelta(days=1)
    return next_day

class SecurityMasterModifier:
    def __init__(self, filename):
        self.filename = filename
        self.fieldnames = []
        self.data = []

    def read_file(self):
        """Reads the CSV file into self.data (list of dicts) and stores fieldnames."""
        with open(self.filename, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            self.fieldnames = reader.fieldnames
            self.data = list(reader)
        print(f"Read {len(self.data)} rows with {len(self.fieldnames)} fields.")

    def detect_type(self, value):
        """
        Detects the type of the given string value.
        Returns one of: "integer", "float", "date", "string".
        Returns None if the value is empty.
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
        """
        Generates a dummy value (as a string) based on the detected type.
        """
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

    def modify_rows(self):
        """
        Prompts for the number of rows to modify and the number of fields to change (only from column 9 onward).
        For any row where at least one field is modified, the code reads its APPLIED_DATE from the file,
        adds one business day to it, and writes the new date back.
        Empty fields are left unchanged.
        """
        try:
            num_rows_to_modify = int(input("Enter the number of rows to modify: "))
            num_fields_to_change = int(input("Enter the number of fields to change in each selected row: "))
        except ValueError:
            print("Invalid input. Please enter integer values.")
            return

        total_rows = len(self.data)
        if num_rows_to_modify > total_rows:
            print("Number of rows to modify exceeds total rows. Modifying all rows.")
            num_rows_to_modify = total_rows

        # Only columns from index 8 (i.e. column 9) until the one before APPLIED_DATE are modifiable.
        modifiable_fields = self.fieldnames[8:-1]
        rows_indices = random.sample(range(total_rows), num_rows_to_modify)

        for idx in rows_indices:
            row_modified = False
            if num_fields_to_change > len(modifiable_fields):
                fields_to_change = modifiable_fields
            else:
                fields_to_change = random.sample(modifiable_fields, num_fields_to_change)

            for field in fields_to_change:
                original_value = self.data[idx][field]
                if original_value.strip() == "":
                    continue  # Leave empty fields unchanged.
                typ = self.detect_type(original_value)
                if typ is None:
                    continue
                new_value = self.generate_dummy_value_by_type(typ)
                if new_value != original_value:
                    self.data[idx][field] = new_value
                    row_modified = True

            # If at least one field was updated, update APPLIED_DATE.
            if row_modified:
                applied_date_str = self.data[idx].get("APPLIED_DATE", "").strip()
                if applied_date_str:
                    try:
                        current_date = datetime.datetime.strptime(applied_date_str, "%Y-%m-%d").date()
                        new_date = add_business_day(current_date)
                        self.data[idx]["APPLIED_DATE"] = new_date.isoformat()
                    except Exception as e:
                        print(f"Error updating APPLIED_DATE for row {idx}: {e}")

        print(f"Modified {num_rows_to_modify} rows. Only rows with changes had APPLIED_DATE updated.")

    def save_file(self, output_filename):
        """Saves the modified data to a new CSV file."""
        with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in self.data:
                writer.writerow(row)
        print(f"Modified data saved to {output_filename}.")

# Usage example:
if __name__ == "__main__":
    input_file = input("Enter the input CSV filename: ")
    output_file = input("Enter the output CSV filename: ")
    
    modifier = SecurityMasterModifier(input_file)
    modifier.read_file()
    modifier.modify_rows()
    modifier.save_file(output_file)