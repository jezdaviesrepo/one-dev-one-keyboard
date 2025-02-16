import csv
import random
import string
import datetime
from faker import Faker

# Initialize Faker
fake = Faker()

# Global dictionary to store empty-field patterns per (asset_class, asset_group) pair.
empty_pattern = {}

# -------------------------------
# Company Name Generation
# -------------------------------
def generate_company_names(n):
    companies = set()
    while len(companies) < n:
        companies.add(fake.company())
    return list(companies)

# Pool of company names (adjust pool size as needed)
COMPANY_POOL_SIZE = 200
company_names = generate_company_names(COMPANY_POOL_SIZE)

# -------------------------------
# Currency Generator (ISO codes)
# -------------------------------
CURRENCY_CODES = [
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "SEK", "NZD",
    "MXN", "SGD", "HKD", "NOK", "KRW", "TRY", "RUB", "INR", "BRL", "ZAR"
]

def generate_currency():
    return random.choice(CURRENCY_CODES)

# -------------------------------
# Asset Class and Asset Group Generators
# -------------------------------
ASSET_CLASSES = [
    "Equity", "Fixed Income", "Commodity", "Real Estate", "Cash", "Derivatives"
]

ASSET_GROUPS = {
    "Equity": ["Domestic Equity", "International Equity", "Emerging Markets Equity"],
    "Fixed Income": ["Government Bonds", "Corporate Bonds", "Municipal Bonds", "High Yield Bonds"],
    "Commodity": ["Energy", "Metals", "Agriculture", "Livestock"],
    "Real Estate": ["Commercial", "Residential", "Industrial"],
    "Cash": ["Short-Term Instruments", "Money Market"],
    "Derivatives": ["Options", "Futures", "Swaps"]
}

def generate_asset_class():
    return random.choice(ASSET_CLASSES)

def generate_asset_group(asset_class):
    return random.choice(ASSET_GROUPS[asset_class])

# -------------------------------
# Special Identifier Generators
# -------------------------------
def compute_figi_check_digit(figi_without_check: str) -> str:
    total = 0
    for ch in figi_without_check:
        if ch.isdigit():
            total += int(ch)
        elif ch.isalpha():
            total += ord(ch) - ord('A') + 10
    return str(total % 10)

def generate_figi() -> str:
    prefix = "BBG"
    allowed_chars = string.ascii_uppercase + string.digits
    figi_without_check = prefix + ''.join(random.choices(allowed_chars, k=8))
    return figi_without_check + compute_figi_check_digit(figi_without_check)

def char_to_value(c: str) -> int:
    if c.isdigit():
        return int(c)
    elif c.isalpha():
        return ord(c.upper()) - ord('A') + 10
    elif c in {"*", "@", "#"}:
        return {"*": 36, "@": 37, "#": 38}[c]
    else:
        raise ValueError(f"Invalid character in CUSIP: {c}")

def compute_cusip_check_digit(cusip_without_check: str) -> str:
    total = 0
    for i, c in enumerate(cusip_without_check):
        multiplier = 2 if (i + 1) % 2 == 0 else 1
        product = char_to_value(c) * multiplier
        if product > 9:
            product -= 9
        total += product
    return str((10 - (total % 10)) % 10)

def generate_cusip() -> str:
    allowed_chars = string.ascii_uppercase + string.digits
    cusip_without_check = ''.join(random.choices(allowed_chars, k=8))
    return cusip_without_check + compute_cusip_check_digit(cusip_without_check)

def compute_sedol_check_digit(sedol_without_check: str) -> str:
    weights = [1, 3, 1, 7, 3, 9]
    total = 0
    for i, ch in enumerate(sedol_without_check):
        if ch.isdigit():
            value = int(ch)
        elif ch.isalpha():
            value = ord(ch.upper()) - ord('A') + 10
        else:
            raise ValueError(f"Invalid character in SEDOL: {ch}")
        total += weights[i] * value
    return str((10 - (total % 10)) % 10)

def generate_sedol() -> str:
    allowed_chars = string.ascii_uppercase + string.digits
    sedol_without_check = ''.join(random.choices(allowed_chars, k=6))
    return sedol_without_check + compute_sedol_check_digit(sedol_without_check)

def convert_isin_char(c: str) -> str:
    if c.isdigit():
        return c
    else:
        return str(ord(c.upper()) - ord('A') + 10)

def compute_isin_check_digit(isin_without_check: str) -> str:
    converted = "".join(convert_isin_char(c) for c in isin_without_check)
    digits = list(map(int, converted))
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            doubled = d * 2
            if doubled > 9:
                doubled -= 9
            total += doubled
        else:
            total += d
    return str((10 - (total % 10)) % 10)

def generate_isin() -> str:
    country_codes = ["US", "GB", "JP", "DE", "FR", "CA", "AU", "CH"]
    country = random.choice(country_codes)
    allowed_chars = string.ascii_uppercase + string.digits
    body = ''.join(random.choices(allowed_chars, k=9))
    isin_without_check = country + body
    return isin_without_check + compute_isin_check_digit(isin_without_check)

# -------------------------------
# Dummy Data Generators for Consistent Types
# -------------------------------
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_dummy_value_by_type(typ):
    if typ == "string":
        return random_string(10)
    elif typ == "integer":
        return str(random.randint(0, 100))
    elif typ == "float":
        return str(round(random.uniform(0, 100), 2))
    elif typ == "date":
        start_date = datetime.date(2000, 1, 1)
        end_date = datetime.date(2025, 12, 31)
        delta_days = (end_date - start_date).days
        random_days = random.randint(0, delta_days)
        return (start_date + datetime.timedelta(days=random_days)).isoformat()
    else:
        return ""

# -------------------------------
# CSV Field and Data Row Generation
# -------------------------------
def generate_security_master_fields(num_dummy_fields):
    """
    Returns a list of field names.
    Fixed fields (always present):
      FIGI, CUSIP, SEDOL, ISIN, COMPANY_NAME, CURRENCY, ASSET_CLASS, ASSET_GROUP
    Dummy fields: count = num_dummy_fields, named FIELD_0009, FIELD_0010, ...
    Last fixed field: APPLIED_DATE
    Total columns = 8 + num_dummy_fields + 1.
    """
    fixed_fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    dummy_fields = [f"FIELD_{i:04d}" for i in range(9, 9 + num_dummy_fields)]
    return fixed_fields + dummy_fields + ["APPLIED_DATE"]

def generate_dummy_data_row(fields, dummy_field_types):
    """
    Generates a single row as a dictionary.
    - Fixed fields (first 8) are populated using dedicated generators.
    - Dummy fields (columns 9 to second-last) are populated with type-consistent values,
      using a consistent empty pattern per asset class/asset group pair.
    - The last field, APPLIED_DATE, is set to the current date.
    """
    row = {}
    row["FIGI"] = generate_figi()
    row["CUSIP"] = generate_cusip()
    row["SEDOL"] = generate_sedol()
    row["ISIN"] = generate_isin()
    row["COMPANY_NAME"] = random.choice(company_names)
    row["CURRENCY"] = generate_currency()
    
    asset_class = generate_asset_class()
    row["ASSET_CLASS"] = asset_class
    asset_group = generate_asset_group(asset_class)
    row["ASSET_GROUP"] = asset_group

    # Dummy fields are from index 8 to the second-last field.
    dummy_fields = fields[8:-1]
    pair_key = (asset_class, asset_group)
    if pair_key not in empty_pattern:
        total_dummy = len(dummy_fields)
        n_empty = random.randint(int(0.4 * total_dummy), int(0.5 * total_dummy))
        empty_set = set(random.sample(dummy_fields, n_empty))
        empty_pattern[pair_key] = empty_set
    else:
        empty_set = empty_pattern[pair_key]

    for field in dummy_fields:
        if field in empty_set:
            row[field] = ""
        else:
            col_type = dummy_field_types[field]
            row[field] = generate_dummy_value_by_type(col_type)
    
    # Set the APPLIED_DATE field to today's date.
    row["APPLIED_DATE"] = datetime.date.today().isoformat()
    return row

def main():
    try:
        row_count = int(input("Enter the number of rows: "))
        num_dummy_fields = int(input("Enter the number of dummy columns: "))
    except ValueError:
        print("Invalid input. Please enter integers.")
        return

    fields = generate_security_master_fields(num_dummy_fields)
    
    # For dummy fields (columns 9 to second-last), assign a fixed type per column.
    dummy_fields = fields[8:-1]
    data_types = ['string', 'integer', 'float', 'date']
    dummy_field_types = {field: random.choice(data_types) for field in dummy_fields}

    # Create filename based on today's date in YYYY-MM-DD format.
    today_str = datetime.date.today().isoformat()
    output_file = f"response_{today_str}.csv"
    
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for _ in range(row_count):
            row = generate_dummy_data_row(fields, dummy_field_types)
            writer.writerow(row)
    
    print(f"CSV file '{output_file}' generated with {row_count} rows and {len(fields)} columns.")

if __name__ == "__main__":
    main()