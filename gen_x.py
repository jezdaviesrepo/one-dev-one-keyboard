import csv
import random
import string
import datetime
import os
import re
from faker import Faker

fake = Faker()

def generate_company_name() -> str:
    return fake.company()

def generate_currency() -> str:
    currencies = [
        "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "SEK", "NZD",
        "MXN", "SGD", "HKD", "NOK", "KRW", "TRY", "RUB", "INR", "BRL", "ZAR"
    ]
    return random.choice(currencies)

def generate_asset_class() -> str:
    asset_classes = ["Equity", "Fixed Income", "Commodity", "Real Estate", "Cash", "Derivatives"]
    return random.choice(asset_classes)

def generate_asset_group(asset_class: str) -> str:
    asset_groups = {
        "Equity": ["Domestic Equity", "International Equity", "Emerging Markets Equity"],
        "Fixed Income": ["Government Bonds", "Corporate Bonds", "Municipal Bonds", "High Yield Bonds"],
        "Commodity": ["Energy", "Metals", "Agriculture", "Livestock"],
        "Real Estate": ["Commercial", "Residential", "Industrial"],
        "Cash": ["Short-Term Instruments", "Money Market"],
        "Derivatives": ["Options", "Futures", "Swaps"]
    }
    return random.choice(asset_groups.get(asset_class, ["General"]))

def generate_dummy_field_names(num_dummy_fields):
    # Dummy fields now start at FIELD_0001
    return [f"FIELD_{i:04d}" for i in range(1, num_dummy_fields + 1)]

def generate_dummy_field_types(dummy_fields):
    possible_types = ["string", "integer", "float", "date"]
    return {field: random.choice(possible_types) for field in dummy_fields}

class SecurityMasterGeneratorFromSOI:
    def __init__(self, soi_filename, vendor_name, num_dummy_fields, underscore_count):
        """
        soi_filename: input CSV file (soi.csv) containing the fixed columns.
        vendor_name: vendor name provided by the user.
        num_dummy_fields: number of dummy columns to generate.
        underscore_count: number of underscores to prepend to dummy field headers.
        """
        self.soi_filename = soi_filename
        self.vendor_name = vendor_name
        self.num_dummy_fields = num_dummy_fields
        self.underscore_count = underscore_count
        self.soi_fixed_fields = []  # Expected fixed fields from soi.csv (should be 4: FIGI, CUSIP, SEDOL, ISIN)
        self.rows = []              # Rows read from soi.csv
        # Dictionary mapping (asset_class, asset_group) to a set of dummy field base names to leave empty.
        self.empty_pattern = {}

    def read_soi_file(self):
        """Reads the soi.csv file and stores its fixed fields and rows."""
        with open(self.soi_filename, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            self.soi_fixed_fields = reader.fieldnames  # Expected to be 4 columns.
            self.rows = list(reader)
        print(f"Loaded {len(self.rows)} rows from {self.soi_filename} with fixed fields: {self.soi_fixed_fields}")

    def generate_output(self):
        """
        Generates a new CSV file with the following structure:
          1. Fixed columns from soi.csv (unchanged).
          2. Additional generated fixed columns: COMPANY_NAME, CURRENCY, ASSET_CLASS, ASSET_GROUP.
          3. Dummy columns with headers prefixed by the specified underscores.
          4. A final APPLIED_DATE column (no underscores).
        Output filename: <vendor_name>_<today's date>.csv, saved in the root directory.
        Returns the output filename.
        """
        # Step 1: Fixed fields from soi.csv.
        fixed = self.soi_fixed_fields  # e.g. 4 columns.
        
        # Step 2: Additional fixed columns.
        generated = ["COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
        
        # Step 3: Dummy columns.
        dummy_fields = generate_dummy_field_names(self.num_dummy_fields)
        dummy_prefix = "_" * self.underscore_count
        dummy_fields_prefixed = [dummy_prefix + field for field in dummy_fields]
        
        # Step 4: Final column.
        final = ["APPLIED_DATE"]
        
        # Combined header.
        output_headers = fixed + generated + dummy_fields_prefixed + final
        
        current_date = datetime.date.today().isoformat()
        output_filename = f"{self.vendor_name}_{current_date}.csv"
        
        # Generate dummy field types.
        dummy_field_types = generate_dummy_field_types(dummy_fields)
        
        with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=output_headers)
            writer.writeheader()
            for fixed_row in self.rows:
                new_row = {}
                # 1. Copy fixed fields from soi.csv.
                for field in fixed:
                    new_row[field] = fixed_row[field]
                # 2. Generate additional fixed fields.
                company_name = generate_company_name()
                currency = generate_currency()
                asset_class = generate_asset_class()
                asset_group = generate_asset_group(asset_class)
                new_row["COMPANY_NAME"] = company_name
                new_row["CURRENCY"] = currency
                new_row["ASSET_CLASS"] = asset_class
                new_row["ASSET_GROUP"] = asset_group
                
                # 3. Dummy fields: determine which dummy fields should be empty based on the (asset_class, asset_group) pair.
                pair = (asset_class, asset_group)
                if pair not in self.empty_pattern:
                    total_dummy = len(dummy_fields)
                    # Choose a percentage between 40% and 60%.
                    percent = random.uniform(0.4, 0.6)
                    num_empty = int(round(percent * total_dummy))
                    # Randomly select that many dummy field base names.
                    self.empty_pattern[pair] = set(random.sample(dummy_fields, num_empty))
                empty_set = self.empty_pattern[pair]
                dummy_values = {}
                for field in dummy_fields:
                    if field in empty_set:
                        dummy_values[field] = ""
                    else:
                        dummy_values[field] = self._generate_value(dummy_field_types[field])
                for field, value in dummy_values.items():
                    new_row[dummy_prefix + field] = value
                
                # 4. Set APPLIED_DATE.
                new_row["APPLIED_DATE"] = current_date
                writer.writerow(new_row)
        print(f"Output file '{output_filename}' generated with {len(self.rows)} rows and {len(output_headers)} columns.")
        return output_filename

    def _generate_value(self, field_type: str) -> str:
        if field_type == "integer":
            return str(random.randint(0, 100))
        elif field_type == "float":
            return str(round(random.uniform(0, 100), 2))
        elif field_type == "date":
            start_date = datetime.date(2000, 1, 1)
            end_date = datetime.date(2025, 12, 31)
            delta = (end_date - start_date).days
            return (start_date + datetime.timedelta(days=random.randint(0, delta))).isoformat()
        else:
            return ''.join(random.choices(string.ascii_letters, k=10))
    
    def run_rule_engine(self, inventory_filename: str, report_filename: str):
        """
        Reads the generated inventory CSV file (which contains:
            FIGI, CUSIP, SEDOL, ISIN, COMPANY_NAME, CURRENCY, ASSET_CLASS, ASSET_GROUP, APPLIED_DATE)
        and checks the four security key fields (FIGI, CUSIP, SEDOL, ISIN) for errors.
          - If a field is empty, records a warning.
          - If a field does not match the expected regex pattern, records an error.
        Also computes a unique composite key for each row by concatenating:
          FIGI, CUSIP, SEDOL, ISIN, COMPANY_NAME, CURRENCY, ASSET_CLASS, ASSET_GROUP, and APPLIED_DATE.
        Writes a report CSV file with columns:
           RowNumber, UniqueKey, Field, FieldValue, Issue, Message
        """
        # Expected regex patterns.
        patterns = {
            "FIGI": r"^BBG[A-Z0-9]{8}\d$",
            "CUSIP": r"^[A-Z0-9*@#]{9}$",
            "SEDOL": r"^[A-Z0-9]{7}$",
            "ISIN": r"^[A-Z]{2}[A-Z0-9]{9}\d$"
        }
        issues = []
        with open(inventory_filename, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row_index, row in enumerate(reader, start=1):
                # Compute unique key: concatenate the 9 fields (4 from soi, 4 additional, and APPLIED_DATE)
                unique_key = (
                    row.get("FIGI", "") + "|" +
                    row.get("CUSIP", "") + "|" +
                    row.get("SEDOL", "") + "|" +
                    row.get("ISIN", "") + "|" +
                    row.get("COMPANY_NAME", "") + "|" +
                    row.get("CURRENCY", "") + "|" +
                    row.get("ASSET_CLASS", "") + "|" +
                    row.get("ASSET_GROUP", "") + "|" +
                    row.get("APPLIED_DATE", "")
                )
                # Validate the 4 key fields.
                for field, pattern in patterns.items():
                    value = row.get(field, "")
                    if value == "":
                        issues.append({
                            "RowNumber": row_index,
                            "UniqueKey": unique_key,
                            "Field": field,
                            "FieldValue": value,
                            "Issue": "Warning",
                            "Message": "Field is empty."
                        })
                    else:
                        if not re.match(pattern, value):
                            issues.append({
                                "RowNumber": row_index,
                                "UniqueKey": unique_key,
                                "Field": field,
                                "FieldValue": value,
                                "Issue": "Error",
                                "Message": f"Value does not match expected pattern for {field}."
                            })
        with open(report_filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["RowNumber", "UniqueKey", "Field", "FieldValue", "Issue", "Message"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for issue in issues:
                writer.writerow(issue)
        print(f"Rule engine report generated: {report_filename}")

if __name__ == "__main__":
    soi_filename = "soi.csv"
    try:
        num_dummy_fields = int(input("Enter the number of dummy columns to add: "))
    except ValueError:
        print("Invalid input. Please enter an integer for the number of dummy columns.")
        exit(1)
    vendor_name = input("Enter the vendor name: ").strip()
    try:
        underscore_count = int(input("Enter the number of underscores to add to dummy field names: "))
    except ValueError:
        print("Invalid input. Please enter an integer for underscore count.")
        exit(1)
    
    generator = SecurityMasterGeneratorFromSOI(soi_filename, vendor_name, num_dummy_fields, underscore_count)
    generator.read_soi_file()
    inventory_filename = generator.generate_output()
    
    report_filename = f"soi_rule_engine_report_{datetime.date.today().isoformat()}.csv"
    generator.run_rule_engine(inventory_filename, report_filename)