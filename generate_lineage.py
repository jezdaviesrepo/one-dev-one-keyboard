import csv
import json
import os

def generate_vendor_mapping(vendor_filename):
    """
    Reads the vendor CSV file, generates a mapping dictionary that maps
    vendor field names to model field names (lower snake case), and
    creates a mapping record that notes which vendor file and field
    produced each model field.
    
    For example, if the vendor file is "bloomberg_2025-02-17.csv" and
    it contains a dummy field "__FIELD_01", then the model field will be
    "field_01" and the mapping record will include:
    
       "field_01": "bloomberg_2025-02-17.csv:__FIELD_01"
    
    Fixed fields (e.g. FIGI, CUSIP, etc.) are mapped by simply lowercasing.
    Dummy fields (those starting with underscores) have their leading underscores stripped.
    """
    with open(vendor_filename, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames

    mapping = {}
    mapping_record = {}

    # Example list of fixed fields (you can adjust this list as needed)
    fixed_fields = {"FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP", "APPLIED_DATE"}

    for field in headers:
        if field.upper() in fixed_fields:
            model_field = field.lower()
            mapping[field] = model_field
            mapping_record[model_field] = f"{os.path.basename(vendor_filename)}:{field}"
        elif field.startswith("_"):  # assume dummy fields start with underscores
            model_field = field.lstrip("_").lower()
            mapping[field] = model_field
            mapping_record[model_field] = f"{os.path.basename(vendor_filename)}:{field}"
        else:
            # If the field is not in fixed_fields and does not start with an underscore,
            # you might decide to leave it unchanged or ignore it.
            model_field = field.lower()
            mapping[field] = model_field
            mapping_record[model_field] = f"{os.path.basename(vendor_filename)}:{field}"

    return mapping, mapping_record

if __name__ == "__main__":
    vendor_filename = input("Enter vendor CSV filename: ").strip()
    mapping, mapping_record = generate_vendor_mapping(vendor_filename)
    
    print("Generated Mapping:")
    for k, v in mapping.items():
        print(f"{k} -> {v}")
    
    print("\nMapping Record:")
    for model_field, record in mapping_record.items():
        print(f"{model_field} = {record}")

    # Save the mapping record to a JSON file
    output_filename = f"mapping_{os.path.basename(vendor_filename)}.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(mapping_record, f, indent=4)
    print(f"\nMapping record saved to {output_filename}")