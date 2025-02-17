import csv
import os
import datetime

def extract_vendor_name(filename):
    """
    Extract vendor name from filename.
    If an underscore is present, vendor name is the part before the first underscore;
    otherwise, it's the filename without its extension.
    """
    base = os.path.basename(filename)
    if "_" in base:
        return base.split("_")[0]
    else:
        return os.path.splitext(base)[0]

def is_dummy_field(field_name):
    """
    Returns True if the field is considered a dummy field:
    It starts with an underscore and contains "FIELD".
    """
    return field_name.startswith("_") and "FIELD" in field_name

def generate_vendor_mapping(headers):
    """
    Generates a mapping dictionary from vendor field names to model field names.
    - Fixed fields: lowercased.
    - Dummy fields: strip all leading underscores then lowercase.
    """
    mapping = {}
    for header in headers:
        if is_dummy_field(header):
            mapping[header] = header.lstrip("_").lower()
        else:
            mapping[header] = header.lower()
    return mapping

def map_vendor_row(row, mapping):
    """
    Maps a vendor row (dictionary) to the central model format using the provided mapping.
    Only fields present in the mapping are included.
    """
    return { mapping[k]: v for k, v in row.items() if k in mapping }

def read_vendor_file(filepath):
    """
    Reads a CSV file and returns its headers and list of rows.
    """
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        rows = list(reader)
    return headers, rows

def write_model_file(output_filepath, model_fieldnames, mapped_rows):
    """
    Writes mapped rows to a CSV file with model_fieldnames as headers.
    """
    with open(output_filepath, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=model_fieldnames)
        writer.writeheader()
        for row in mapped_rows:
            writer.writerow(row)
    print(f"Generated model file: {output_filepath}")

def process_file(input_filepath, output_dir):
    headers, rows = read_vendor_file(input_filepath)
    mapping = generate_vendor_mapping(headers)
    mapped_rows = [map_vendor_row(row, mapping) for row in rows]
    
    # Model fieldnames in the same order as the vendor file's headers.
    model_fieldnames = [mapping[h] for h in headers]
    
    # Use the same filename as input.
    base = os.path.basename(input_filepath)
    output_filepath = os.path.join(output_dir, base)
    
    write_model_file(output_filepath, model_fieldnames, mapped_rows)

def main():
    store_dir = "store"
    model_dir = "inventory"
    os.makedirs(model_dir, exist_ok=True)
    
    # Process all CSV files in the store directory.
    for filename in os.listdir(store_dir):
        if filename.lower().endswith(".csv"):
            input_filepath = os.path.join(store_dir, filename)
            process_file(input_filepath, model_dir)
    
    print("All files processed.")

if __name__ == "__main__":
    main()