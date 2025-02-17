import os
import random

# Fixed (core) fields for the security model.
fixed_fields = [
    {"name": "figi", "label": "FIGI", "type": "string"},
    {"name": "cusip", "label": "CUSIP", "type": "string"},
    {"name": "sedol", "label": "SEDOL", "type": "string"},
    {"name": "isin", "label": "ISIN", "type": "string"},
    {"name": "company_name", "label": "Company Name", "type": "string"},
    {"name": "currency", "label": "Currency", "type": "string"},
    {"name": "asset_class", "label": "Asset Class", "type": "string"},
    {"name": "asset_group", "label": "Asset Group", "type": "string"},
    {"name": "applied_date", "label": "Applied Date", "type": "date"}
]

# Preset possible types.
possible_types = ["string", "integer", "float", "date", "boolean"]

def snake_to_label(snake_str):
    # Convert lower_snake_case to Title Case with spaces.
    return ' '.join(word.capitalize() for word in snake_str.split('_'))

def generate_additional_fields(num_fields: int):
    additional_fields = []
    for i in range(1, num_fields + 1):
        # Automatically generate field name as "field_<i>"
        field_name = f"field_{i}"
        # Generate label from field name (e.g. "field_1" -> "Field 1")
        label = snake_to_label(field_name)
        # Randomly choose a type from possible_types.
        field_type = random.choice(possible_types)
        additional_fields.append({
            "name": field_name,
            "label": label,
            "type": field_type
        })
    return additional_fields

def generate_config_and_model():
    try:
        num = int(input("How many additional fields do you want? "))
    except ValueError:
        print("Invalid input. Using 0 additional fields.")
        num = 0

    additional_fields = generate_additional_fields(num)
    all_fields = fixed_fields + additional_fields

    # Create the model folder if it doesn't exist.
    model_dir = "model"
    os.makedirs(model_dir, exist_ok=True)

    # Write out the configuration file.
    config_path = os.path.join(model_dir, "security_fields_config.py")
    with open(config_path, "w") as config_file:
        config_file.write("security_fields = [\n")
        for field_def in all_fields:
            config_file.write(f"    {field_def},\n")
        config_file.write("]\n")
    print(f"Configuration file generated at: {config_path}")

    # Map our possible types to Python types.
    type_mapping = {
        "string": "Optional[str]",
        "integer": "Optional[int]",
        "float": "Optional[float]",
        "date": "Optional[str]",  # For simplicity, we'll use str for dates.
        "boolean": "Optional[bool]"
    }

    # Write out the model file using dataclasses.
    model_path = os.path.join(model_dir, "security_model.py")
    with open(model_path, "w") as model_file:
        model_file.write("from dataclasses import dataclass, field\n")
        model_file.write("from typing import Optional, Dict\n\n")
        model_file.write("@dataclass\n")
        model_file.write("class Security:\n")
        # Write each field from the configuration as an attribute.
        for field_def in all_fields:
            py_type = type_mapping.get(field_def["type"], "Optional[str]")
            model_file.write(f"    {field_def['name']}: {py_type} = None\n")
        # Optionally, if you want to capture any extra vendor fields:
        model_file.write("\n    additional_fields: Dict[str, str] = field(default_factory=dict)\n")
    print(f"Model file generated at: {model_path}")

if __name__ == "__main__":
    generate_config_and_model()