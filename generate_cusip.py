import random
import string

def char_to_value(c: str) -> int:
    """
    Converts a single character to its numeric value for CUSIP calculation.
    - Digits: as their integer value.
    - Letters: A -> 10, B -> 11, ..., Z -> 35.
    """
    if c.isdigit():
        return int(c)
    elif c.isalpha():
        return ord(c.upper()) - ord('A') + 10
    # Optionally, handle special characters if needed:
    elif c == '*':
        return 36
    elif c == '@':
        return 37
    elif c == '#':
        return 38
    else:
        raise ValueError(f"Invalid character in CUSIP: {c}")

def compute_cusip_check_digit(cusip_without_check: str) -> str:
    """
    Computes the check digit for a CUSIP using the following algorithm:
    - Multiply each character's numeric value by 1 (if its position is odd) or 2 (if even).
    - If a product is greater than 9, subtract 9 from it.
    - Sum all the products and compute the check digit as:
        (10 - (total % 10)) % 10
    """
    total = 0
    for i, c in enumerate(cusip_without_check):
        # Positions are 1-indexed in the algorithm:
        multiplier = 2 if (i + 1) % 2 == 0 else 1
        value = char_to_value(c)
        product = value * multiplier
        # If product is two-digit, sum its digits (or subtract 9)
        if product > 9:
            product -= 9
        total += product
    check_digit = (10 - (total % 10)) % 10
    return str(check_digit)

def generate_cusip() -> str:
    """
    Generates a single random CUSIP.
    - The first 8 characters are randomly selected from uppercase letters and digits.
    - The 9th character is the check digit computed from the first 8 characters.
    """
    allowed_chars = string.ascii_uppercase + string.digits
    cusip_without_check = ''.join(random.choices(allowed_chars, k=8))
    check_digit = compute_cusip_check_digit(cusip_without_check)
    return cusip_without_check + check_digit

def generate_cusips(n: int):
    """
    Generates 'n' random CUSIP strings.
    """
    return [generate_cusip() for _ in range(n)]

if __name__ == '__main__':
    try:
        count = int(input("How many CUSIPs would you like to generate? "))
    except ValueError:
        print("Please enter a valid integer.")
        exit(1)
    
    cusips = generate_cusips(count)
    print("\nGenerated CUSIPs:")
    for cusip in cusips:
        print(cusip)