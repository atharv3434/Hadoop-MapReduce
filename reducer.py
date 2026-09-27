#!/usr/bin/env python3
"""Word count reducer — Hadoop Streaming contract.

Hadoop guarantees the reducer receives its input already sorted by key,
with all values for the same key arriving consecutively (this is exactly
what the shuffle & sort phase produces). This reducer relies on that
guarantee — it just needs to notice when the key changes, following the
standard Hadoop Streaming reducer pattern.
"""

import sys


def main():
    current_key = None
    current_count = 0

    for line in sys.stdin:
        key, value = line.rstrip("\n").split("\t", 1)
        count = int(value)

        if key == current_key:
            current_count += count
        else:
            if current_key is not None:
                print(f"{current_key}\t{current_count}")
            current_key = key
            current_count = count

    if current_key is not None:
        print(f"{current_key}\t{current_count}")


if __name__ == "__main__":
    main()
