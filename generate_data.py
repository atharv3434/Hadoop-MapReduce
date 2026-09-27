"""Generate sample input data for the two bundled MapReduce jobs.

This project ships with pre-generated data already in place
(data/sample_text.txt, data/sales_log.csv), so you don't need to run this
to try the project out. Run it again for different/larger sample data.

Usage:
    python data/generate_data.py [--n-sales-rows 5000] [--seed 42]
"""

import argparse
import os

import numpy as np

# A short public-domain-style passage repeated/varied for the word count job.
WORDCOUNT_SENTENCES = [
    "the quick brown fox jumps over the lazy dog",
    "hadoop mapreduce splits work into map tasks and reduce tasks",
    "the map phase processes input records in parallel across the cluster",
    "the shuffle and sort phase groups values by key between map and reduce",
    "the reduce phase aggregates values for each key and writes the output",
    "distributed computing lets clusters of machines process huge datasets",
    "the quick fox and the lazy dog appear again in this sentence",
    "big data frameworks like hadoop and spark process data at scale",
    "each mapper reads a split of the input and emits key value pairs",
    "each reducer receives all values for a given key in sorted order",
]

PRODUCTS = {
    "laptop": "electronics", "phone": "electronics", "headphones": "electronics",
    "desk": "furniture", "chair": "furniture", "lamp": "furniture",
    "novel": "books", "cookbook": "books", "notebook": "office_supplies",
    "pen": "office_supplies", "monitor": "electronics", "bookshelf": "furniture",
}


def generate_wordcount_text(rng, n_lines=200):
    lines = []
    for _ in range(n_lines):
        sentence = WORDCOUNT_SENTENCES[rng.integers(0, len(WORDCOUNT_SENTENCES))]
        lines.append(sentence)
    return "\n".join(lines)


def generate_sales_log(rng, n_rows=5000):
    products = list(PRODUCTS.keys())
    lines = ["date,product,category,amount"]
    for _ in range(n_rows):
        product = products[rng.integers(0, len(products))]
        category = PRODUCTS[product]
        amount = round(float(rng.uniform(5, 500)), 2)
        month = int(rng.integers(1, 13))
        day = int(rng.integers(1, 28))
        date = f"2026-{month:02d}-{day:02d}"
        lines.append(f"{date},{product},{category},{amount}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate sample MapReduce input data.")
    parser.add_argument("--n-wordcount-lines", type=int, default=200)
    parser.add_argument("--n-sales-rows", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    os.makedirs(args.out_dir, exist_ok=True)

    text = generate_wordcount_text(rng, args.n_wordcount_lines)
    text_path = os.path.join(args.out_dir, "sample_text.txt")
    with open(text_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Wrote {args.n_wordcount_lines} lines to {text_path}")

    sales_csv = generate_sales_log(rng, args.n_sales_rows)
    sales_path = os.path.join(args.out_dir, "sales_log.csv")
    with open(sales_path, "w", encoding="utf-8") as f:
        f.write(sales_csv)
    print(f"Wrote {args.n_sales_rows} rows to {sales_path}")


if __name__ == "__main__":
    main()
