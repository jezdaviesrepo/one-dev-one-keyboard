import random
import string

def compute_check_digit(figi_without_check: str) -> str:
    """
    Computes a check digit for the FIGI (first 11 characters).
    Each letter is converted to a number (A=10, B=11, ... Z=35) and each digit is its numeric value.
    The check digit is the sum of these values modulo 10.
    """
    total = 0
    for ch in figi_without_check:
        if ch.isdigit():
            value = int(ch)
        elif ch.isalpha():
            value = ord(ch) - ord('A') + 10
        else:
            value = 0
        total += valueinte
    return str(total % 10)

def generate_figi() -> str:
    """
    Generates a FIGI that starts with 'BBG' followed by 8 random alphanumeric characters,
    and ends with a check digit computed from the first 11 characters.
    Total length is 12 characters.
    """
    # Fixed prefix 'BBG'
    prefix = "BBG"
    # Generate 8 alphanumeric characters (A-Z, 0-9)
    allowed_chars = string.ascii_uppercase + string.digits
    middle = ''.join(random.choices(allowed_chars, k=8))
    
    # Build FIGI without the check digit (3 + 8 = 11 characters)
    figi_without_check = prefix + middle
    # Compute the check digit
    check_digit = compute_check_digit(figi_without_check)
    
    return figi_without_check + check_digit

def generate_figis(n: int):
    """
    Generates 'n' FIGI strings.
    """
    return [generate_figi() for _ in range(n)]

if __name__ == '__main__':
    try:
        count = int(input("How many FIGIs would you like to generate? "))
    except ValueError:
        print("Please enter a valid integer.")
        exit(1)
    
    figis = generate_figis(count)
    print("\nGenerated FIGIs:")
    for figi in figis:
        print(figi)