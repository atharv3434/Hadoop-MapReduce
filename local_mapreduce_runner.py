"""
A local execution engine that replicates Hadoop's actual MapReduce data
flow — input splitting, parallel map tasks, hash-partitioned shuffle &
sort, and parallel reduce tasks — running ordinary Hadoop-Streaming-style
mapper/reducer scripts (subprocesses that read stdin, write stdout).

What this faithfully reproduces from real Hadoop:
- **Input splitting**: the input file is divided into N contiguous line
  ranges ("splits"), each processed by an independent map task — the same
  logical unit of parallel work Hadoop's FileInputFormat creates.
- **Parallel map tasks**: each split is processed by its own mapper.py
  subprocess, run concurrently (via a process pool) — analogous to
  multiple map tasks running across a cluster's nodes.
- **Shuffle & sort**: all mapper output (key, value) pairs are grouped by
  key and sorted, then partitioned by hash(key) % num_reducers — this is
  Hadoop's default HashPartitioner behavior, and the same reason a real
  Hadoop job guarantees all values for one key land on the same reducer.
- **Parallel reduce tasks**: each partition is processed by its own
  reducer.py subprocess, and results are written to part-NNNNN files —
  matching Hadoop's own output file naming.

What is NOT reproduced (this runs on one machine, not a real cluster):
- True multi-node distribution — "parallel" here means multiple processes
  on this one machine, not tasks scheduled across a YARN cluster.
- HDFS storage, block replication, and data locality optimization.
- Fault tolerance — a real cluster re-runs failed tasks on another node;
  this engine has no such recovery.
- YARN resource negotiation/scheduling.

Because the mapper/reducer scripts only use stdin/stdout, the exact same
job files would run unchanged on a real Hadoop cluster via Hadoop
Streaming — only the execution engine underneath differs.

"""

import argparse
import os
import subprocess
import sys
import zlib
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor


def stable_hash(key):
    """A deterministic string hash for partitioning.

    Python's built-in hash() is randomized per-process for security
    (PYTHONHASHSEED) — confirmed by testing: hash("electronics") returned a
    different value on every run. That would make partition assignment
    silently change from run to run, unlike real Hadoop's HashPartitioner,
    which is deterministic. zlib.crc32 gives the same partitioning every
    time, which is what makes a shuffle phase actually reproducible.
    """
    return zlib.crc32(key.encode("utf-8"))


def split_input(input_path, n_splits):
    """Divide the input file into n_splits contiguous line ranges, mimicking
    how Hadoop's FileInputFormat divides a file into input splits.
    """
    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    n_splits = max(1, min(n_splits, len(lines))) if lines else 1
    split_size = max(1, -(-len(lines) // n_splits))  # ceil division
    splits = [lines[i:i + split_size] for i in range(0, len(lines), split_size)]
    return splits


def run_mapper(args):
    """Run one map task: feed a split's lines to mapper.py via stdin,
    collect its (key, value) stdout lines. Runs in a separate process.
    """
    split_lines, mapper_path = args
    input_text = "".join(split_lines)
    result = subprocess.run(
        [sys.executable, mapper_path],
        input=input_text, capture_output=True, text=True, check=True,
    )
    pairs = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        key, value = line.split("\t", 1)
        pairs.append((key, value))
    return pairs


def shuffle_and_sort(all_pairs, n_reducers):
    """Group all mapper output by key, then partition keys across
    n_reducers using hash(key) % n_reducers — Hadoop's default
    HashPartitioner strategy. Within each partition, keys are sorted, which
    is what lets each reducer process use the simple "detect key change"
    pattern real Hadoop Streaming reducers rely on.
    """
    grouped = defaultdict(list)
    for key, value in all_pairs:
        grouped[key].append(value)

    partitions = [[] for _ in range(n_reducers)]
    for key in sorted(grouped.keys()):
        partition_id = stable_hash(key) % n_reducers
        partitions[partition_id].append((key, grouped[key]))

    return partitions


def run_reducer(args):
    """Run one reduce task: feed a partition's sorted (key, [values]) pairs
    to reducer.py via stdin (one "key\\tvalue" line per value, exactly the
    format Hadoop delivers to a Streaming reducer), capture its stdout.
    """
    partition, reducer_path = args
    input_lines = []
    for key, values in partition:
        for value in values:
            input_lines.append(f"{key}\t{value}")
    input_text = "\n".join(input_lines) + ("\n" if input_lines else "")

    result = subprocess.run(
        [sys.executable, reducer_path],
        input=input_text, capture_output=True, text=True, check=True,
    )
    return result.stdout


def run_job(input_path, mapper_path, reducer_path, output_dir, n_mappers=4, n_reducers=2):
    os.makedirs(output_dir, exist_ok=True)

    print(f"[driver] Splitting {input_path} into up to {n_mappers} input splits...")
    splits = split_input(input_path, n_mappers)
    print(f"[driver] Created {len(splits)} input split(s).")

    print(f"[driver] Running {len(splits)} map task(s) in parallel...")
    with ProcessPoolExecutor(max_workers=len(splits)) as pool:
        mapper_results = list(pool.map(run_mapper, [(s, mapper_path) for s in splits]))
    all_pairs = [pair for result in mapper_results for pair in result]
    print(f"[driver] Map phase emitted {len(all_pairs)} intermediate (key, value) pairs.")

    print(f"[driver] Shuffle & sort: grouping by key, partitioning across {n_reducers} reducer(s)...")
    partitions = shuffle_and_sort(all_pairs, n_reducers)
    for i, p in enumerate(partitions):
        print(f"[driver]   Partition {i}: {len(p)} distinct key(s)")

    print(f"[driver] Running {n_reducers} reduce task(s) in parallel...")
    with ProcessPoolExecutor(max_workers=n_reducers) as pool:
        reducer_outputs = list(pool.map(run_reducer, [(p, reducer_path) for p in partitions]))

    for i, output_text in enumerate(reducer_outputs):
        part_path = os.path.join(output_dir, f"part-{i:05d}")
        with open(part_path, "w", encoding="utf-8") as f:
            f.write(output_text)
        print(f"[driver] Wrote {part_path}")

    print("[driver] Job complete.")


def main():
    parser = argparse.ArgumentParser(description="Run a Hadoop-Streaming-style MapReduce job locally.")
    parser.add_argument("--input", required=True, help="Path to the input file")
    parser.add_argument("--mapper", required=True, help="Path to the mapper script")
    parser.add_argument("--reducer", required=True, help="Path to the reducer script")
    parser.add_argument("--output", required=True, help="Output directory (part-NNNNN files written here)")
    parser.add_argument("--n-mappers", type=int, default=4, help="Number of map tasks (input splits)")
    parser.add_argument("--n-reducers", type=int, default=2, help="Number of reduce tasks (partitions)")
    args = parser.parse_args()

    run_job(args.input, args.mapper, args.reducer, args.output, args.n_mappers, args.n_reducers)


if __name__ == "__main__":
    main()
