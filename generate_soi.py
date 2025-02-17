import csv
import random
import string
import datetime

class SOIGenerator:
    def __init__(self):
        # Fixed fields: FIGI, CUSIP, SEDOL, ISIN
        self.fields = ["FIGI", "CUSIP", "SEDOL", "ISIN"]

    def compute_figi_check_digit(self, figi_without_check: str) -> str:
        total = 0
        for ch in figi_without_check:
            if ch.isdigit():
                total += int(ch)
            elif ch.isalpha():
                total += ord(ch) - ord('A') + 10
        return str(total % 10)

    def generate_figi(self) -> str:
        prefix = "BBG"
        allowed_chars = string.ascii_uppercase + string.digits
        figi_without_check = prefix + ''.join(random.choices(allowed_chars, k=8))
        return figi_without_check + self.compute_figi_check_digit(figi_without_check)

    def char_to_value(self, c: str) -> int:
        if c.isdigit():
            return int(c)
        elif c.isalpha():
            return ord(c.upper()) - ord('A') + 10
        elif c in {"*", "@", "#"}:
            return {"*": 36, "@": 37, "#": 38}[c]
        else:
            raise ValueError(f"Invalid character in CUSIP: {c}")

    def compute_cusip_check_digit(self, cusip_without_check: str) -> str:
        total = 0
        for i, c in enumerate(cusip_without_check):
            multiplier = 2 if (i + 1) % 2 == 0 else 1
            product = self.char_to_value(c) * multiplier
            if product > 9:
                product -= 9
            total += product
        return str((10 - (total % 10)) % 10)

    def generate_cusip(self) -> str:
        allowed_chars = string.ascii_uppercase + string.digits
        cusip_without_check = ''.join(random.choices(allowed_chars, k=8))
        return cusip_without_check + self.compute_cusip_check_digit(cusip_without_check)

    def compute_sedol_check_digit(self, sedol_without_check: str) -> str:
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

    def generate_sedol(self) -> str:
        allowed_chars = string.ascii_uppercase + string.digits
        sedol_without_check = ''.join(random.choices(allowed_chars, k=6))
        return sedol_without_check + self.compute_sedol_check_digit(sedol_without_check)

    def convert_isin_char(self, c: str) -> str:
        if c.isdigit():
            return c
        else:
            return str(ord(c.upper()) - ord('A') + 10)

    def compute_isin_check_digit(self, isin_without_check: str) -> str:
        converted = "".join(self.convert_isin_char(c) for c in isin_without_check)
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

    def generate_isin(self) -> str:
        country_codes = ["US", "GB", "JP", "DE", "FR", "CA", "AU", "CH"]
        country = random.choice(country_codes)
        allowed_chars = string.ascii_uppercase + string.digits
        body = ''.join(random.choices(allowed_chars, k=9))
        isin_without_check = country + body
        return isin_without_check + self.compute_isin_check_digit(isin_without_check)

    def generate_row(self) -> dict:
        return {
            self.fields[0]: self.generate_figi(),
            self.fields[1]: self.generate_cusip(),
            self.fields[2]: self.generate_sedol(),
            self.fields[3]: self.generate_isin()
        }

    def generate_csv(self, num_rows: int, output_filename: str = "soi.csv"):
        with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fields)
            writer.writeheader()
            for _ in range(num_rows):
                writer.writerow(self.generate_row())
        print(f"CSV file '{output_filename}' generated with {num_rows} rows and {len(self.fields)} columns.")

if __name__ == "__main__":
    try:
        num_rows = int(input("Enter the number of rows for soi.csv: "))
    except ValueError:
        print("Invalid input. Please enter an integer for the number of rows.")
        exit(1)
    
    generator = SOIGenerator()
    generator.generate_csv(num_rows)