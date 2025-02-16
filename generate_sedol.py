import random
import string

def compute_sedol_check_digit(sedol_without_check: str) -> str:
    """
    Computes the check digit for a 6-character SEDOL code using the weights:
    [1, 3, 1, 7, 3, 9].
    
    Each character is converted:
      - Digits remain as their integer value.
      - Letters: A -> 10, B -> 11, ... Z -> 35.
    
    The check digit is calculated as: (10 - (weighted_sum % 10)) % 10.
    """
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
    check_digit = (10 - (total % 10)) % 10
    return str(check_digit)

def generate_sedol() -> str:
    """
    Generates a single random SEDOL code:
      - First 6 characters: randomly selected from uppercase letters and digits.
      - 7th character: computed check digit.
    Total length is 7 characters.
    """
    allowed_chars = string.ascii_uppercase + string.digits
    sedol_without_check = ''.join(random.choices(allowed_chars, k=6))
    check_digit = compute_sedol_check_digit(sedol_without_check)
    return sedol_without_check + check_digit

def generate_sedols(n: int):
    """
    Generates 'n' random SEDOL codes.
    """
    return [generate_sedol() for _ in range(n)]

if __name__ == '__main__':
    try:
        count = int(input("How many SEDOL codes would you like to generate? "))
    except ValueError:
        print("Please enter a valid integer.")
        exit(1)
    
    sedols = generate_sedols(count)
    print("\nGenerated SEDOL codes:")
    for sedol in sedols:
        print(sedol)