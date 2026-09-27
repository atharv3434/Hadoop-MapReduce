#!/usr/bin/env python3
"""Sales-by-category aggregation reducer — Hadoop Streaming contract.

Relies on the same "sorted, grouped by key" guarantee as
jobs/wordcount/reducer.py. Emits total amount, order count, and average
order value per category.
"""

import sys


def emit(key, total, count):
    average = total / count if count else 0.0
    print(f"{key}\ttotal={total:.2f}\tcount={count}\taverage={average:.2f}")


def main():
    current_key = None
    total = 0.0
    count = 0

    for line in sys.stdin:
        key, value = line.rstrip("\n").split("\t", 1)
        amount = float(value)

        if key == current_key:
            total += amount
            count += 1
        else:
            if current_key is not None:
                emit(current_key, total, count)
            current_key = key
            total = amount
            count = 1

    if current_key is not None:
        emit(current_key, total, count)


if __name__ == "__main__":
    main()
