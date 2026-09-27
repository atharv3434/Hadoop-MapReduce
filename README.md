# Hadoop MapReduce (via Hadoop Streaming), Locally Runnable

An implementation of the **Hadoop MapReduce programming model**, using the
real **Hadoop Streaming** interface — mapper and reducer scripts that only
read stdin and write stdout, exactly the contract Hadoop Streaming uses to
run jobs written in any language. Bundled with a local execution engine
that faithfully replicates Hadoop's actual data flow (input splitting,
parallel map tasks, hash-partitioned shuffle & sort, parallel reduce
tasks), so the whole pipeline runs and is testable without a cluster.

## A note on what this is (and isn't)

This environment can't reach the Apache Hadoop download mirrors (confirmed:
requests to `downloads.apache.org` / `archive.apache.org` are blocked),
Hadoop isn't installable via `apt` or `pip`, and building it from source
needs Maven/protobuf and takes hours — so there's no path to running real
HDFS/YARN daemons here. Rather than fake that, this project does something
genuinely useful instead:

- The **mapper.py / reducer.py scripts** (in `jobs/`) are written to the
  exact Hadoop Streaming contract and would run **unchanged** on a real
  Hadoop cluster (see the exact command in each mapper's docstring).
- The **local execution engine** (`src/local_mapreduce_runner.py`)
  faithfully reproduces Hadoop's real data flow: it splits input into
  chunks, runs mapper subprocesses in parallel, groups and sorts
  intermediate output by key, partitions by `hash(key) % n_reducers`
  (Hadoop's default `HashPartitioner`), and runs reducer subprocesses in
  parallel, writing `part-00000`, `part-00001`, etc. — the same output
  file naming Hadoop itself uses.
- What genuinely differs from a real cluster: this runs multiple
  *processes on one machine*, not tasks distributed across a *YARN
  cluster*; there's no HDFS storage/replication or data-locality
  scheduling; and there's no fault tolerance (a real cluster re-runs
  failed tasks on another node — this engine doesn't).

## Project structure

```
hadoop-mapreduce/
├── requirements.txt
├── data/
│   ├── generate_data.py           # (re)generates the sample inputs
│   ├── sample_text.txt            # sample text for the word count job
│   └── sales_log.csv              # sample CSV for the aggregation job
├── jobs/
│   ├── wordcount/
│   │   ├── mapper.py              # emits "word\t1" per word
│   │   └── reducer.py             # sums counts per word
│   └── sales_aggregation/
│       ├── mapper.py              # emits "category\tamount" per row
│       └── reducer.py             # sums/counts/averages amount per category
├── src/
│   └── local_mapreduce_runner.py  # the local execution engine (the driver)
├── output/
│   ├── wordcount/                 # part-NNNNN files after running
│   └── sales_aggregation/
└── README.md
```

## Setup

```bash
pip install -r requirements.txt
```

## Run the word count job

```bash
python src/local_mapreduce_runner.py \
    --input data/sample_text.txt \
    --mapper jobs/wordcount/mapper.py \
    --reducer jobs/wordcount/reducer.py \
    --output output/wordcount \
    --n-mappers 4 --n-reducers 3
```

```
[driver] Splitting data/sample_text.txt into up to 4 input splits...
[driver] Created 4 input split(s).
[driver] Running 4 map task(s) in parallel...
[driver] Map phase emitted 2248 intermediate (key, value) pairs.
[driver] Shuffle & sort: grouping by key, partitioning across 3 reducer(s)...
[driver]   Partition 0: 22 distinct key(s)
[driver]   Partition 1: 21 distinct key(s)
[driver]   Partition 2: 27 distinct key(s)
[driver] Running 3 reduce task(s) in parallel...
[driver] Wrote output/wordcount/part-00000
...
```

I verified the output two ways: the sum of all counts across every
`part-*` file (2248) exactly matches a plain single-process word count over
the same file, and no word appears in more than one output partition
(confirming the shuffle phase correctly keeps every key's data together).

## Run the sales aggregation job

```bash
python src/local_mapreduce_runner.py \
    --input data/sales_log.csv \
    --mapper jobs/sales_aggregation/mapper.py \
    --reducer jobs/sales_aggregation/reducer.py \
    --output output/sales_aggregation \
    --n-mappers 6 --n-reducers 4
```

```
books            total=217674.98  count=842   average=258.52
electronics      total=425391.46  count=1711  average=248.62
furniture        total=400922.17  count=1611  average=248.87
office_supplies  total=212088.80  count=836   average=253.69
```

Cross-checked against `pandas.groupby("category")["amount"].agg(["sum",
"count", "mean"])` on the same file — every category's sum, count, and
mean match exactly.

## Running these same job files on a real Hadoop cluster

Because the mapper/reducer scripts only touch stdin/stdout, they need no
changes to run on real Hadoop with Hadoop Streaming:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /input/sample_text.txt \
    -output /output/wordcount \
    -mapper jobs/wordcount/mapper.py \
    -reducer jobs/wordcount/reducer.py \
    -file jobs/wordcount/mapper.py \
    -file jobs/wordcount/reducer.py
```

The `-file` flags ship the scripts to every node in the cluster; real
Hadoop then takes over input splitting, distributed execution, and shuffle
& sort exactly as `local_mapreduce_runner.py` simulates them here.

## Writing your own MapReduce job

1. Create a new folder under `jobs/` with a `mapper.py` and `reducer.py`,
   following the same contract: the mapper reads lines from stdin and
   prints `key<TAB>value` lines; the reducer reads stdin assuming it's
   sorted and grouped by key (which the runner guarantees), and prints
   final `key<TAB>result` lines.
2. Make both scripts executable (`chmod +x`), and give them a
   `#!/usr/bin/env python3` shebang line (already done for the two bundled
   jobs).
3. Run them with `src/local_mapreduce_runner.py`, pointing `--mapper` and
   `--reducer` at your new scripts.

## Extending this project

- **More jobs**: a max/min-per-key job, a join between two datasets
  (classic "reduce-side join" MapReduce pattern), or a secondary sort.
- **Combiners**: real Hadoop supports an optional "combiner" (a mini-reduce
  run right after the map phase, before shuffling, to cut network traffic)
  — `local_mapreduce_runner.py` could be extended to run an optional
  `combiner.py` on each mapper's output before shuffling.
- **A real cluster**: if you have access to a real Hadoop installation
  (or a cloud-managed one like EMR or Dataproc), the `jobs/` scripts here
  are ready to submit directly via Hadoop Streaming, as shown above.
