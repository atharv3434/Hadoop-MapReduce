#!/usr/bin/env python3
"""Word count mapper — Hadoop Streaming contract.

Reads text lines from stdin, one record per line (exactly what Hadoop
passes a map task from its assigned input split), and writes
"word<TAB>1" to stdout for every word.

This script has no dependency on this project's local runner — it only
reads stdin and writes stdout, so it is directly usable as-is with real
Hadoop Streaming:

    hadoop jar hadoop-streaming.jar \\
        -input /input/sample_text.txt -output /output/wordcount \\
        -mapper jobs/wordcount/mapper.py -reducer jobs/wordcount/reducer.py \\
        -file jobs/wordcount/mapper.py -file jobs/wordcount/reducer.py
"""

import re
import sys


def main():
    for line in sys.stdin:
        words = re.findall(r"[a-zA-Z']+", line.lower())
        for word in words:
            print(f"{word}\t1")


if __name__ == "__main__":
    main()
