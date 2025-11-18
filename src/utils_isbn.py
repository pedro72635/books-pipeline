import pandas as pd

def validate_isbn13(isbn):
    if pd.isnull(isbn):
        return False
    isbn = str(isbn).replace('-', '').replace(' ', '')
    if len(isbn) != 13 or not isbn.isdigit():
        return False
    total = sum(int(digit) * (1 if i % 2 == 0 else 3) for i, digit in enumerate(isbn[:-1]))
    checksum = (10 - (total % 10)) % 10
    return checksum == int(isbn[-1])