import csv
from faker import Faker

# Initialize Faker
fake = Faker()

# Generate 200 unique company names
company_names = set()
while len(company_names) < 200:
    company_names.add(fake.company())

company_names = list(company_names)

# Write the company names to a CSV file with a field named COMPANY_NAME
output_file = "company_names.csv"
with open(output_file, "w", newline="") as csvfile:
    fieldnames = ["COMPANY_NAME"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for name in company_names:
        writer.writerow({"COMPANY_NAME": name})

print(f"CSV file '{output_file}' generated with 200 unique company names.")