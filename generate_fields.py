def generate_security_master_fields(n=1000):
    """
    Generate a list of 'n' field names in upper snake case.
    For example: FIELD_0001, FIELD_0002, ..., FIELD_1000.
    """
    return [f"FIELD_{i:04d}" for i in range(1, n + 1)]

if __name__ == "__main__":
    fields = generate_security_master_fields()
    # Print each field on a new line
    for field in fields:
        print(field)