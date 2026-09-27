#!/usr/bin/env python3
"""
Sales-by-category aggregation mapper — Hadoop Streaming contract.

Reads CSV lines (date,product,category,amount) from stdin and emits
"category<TAB>amount" for each row. Skips the header line if present.
Like jobs/wordcount/mapper.py, this only touches stdin/stdout, so it is
directly portable to a real Hadoop Streaming job unchanged.
"""

import sys


def main():
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line or line.startswith("date,product,category,amount"):
            continue  # skip header / blank lines

        parts = line.split(",")
        if len(parts) != 4:
            continue  # skip malformed rows rather than crash the whole job

        _date, _product, category, amount = parts
        print(f"{category}\t{amount}")


if __name__ == "__main__":
    main()
