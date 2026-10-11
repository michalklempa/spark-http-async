# Benchmarks

Compares five ways of querying the `http-api` service from PySpark, one HTTP request per batch of questions:

| Variant | How it sends requests |
|---|---|
| `requests` | `rdd.map`, one blocking `requests.post` per row |
| `requests-partition` | `rdd.mapPartitions`, blocking `requests.post` per row, rows of a partition handled one after another |
| `httpx-partition` | `rdd.mapPartitions`, one `httpx.Client` per partition, blocking calls one after another |
| `httpx-partition-async` | `rdd.mapPartitions`, one `httpx.AsyncClient` per partition, whole partition loaded into memory and sent concurrently with `asyncio.gather`, capped by `--max-in-flight` |
| `http-enrich` | `enrich()` from `spark_http_async` (the repository root), built on the `spark-async` library: bounded concurrency, rows streamed in and out of the partition |

## Requirements

- Python 3.13 and [uv](https://docs.astral.sh/uv/)
- JDK 25 and Maven, to run `../../http-api`
- the `spark-async` repository checked out next to `spark-http-async` (as `../../spark-async`), because `pyproject.toml` installs it from that path

## Setup

```bash
uv sync
```

## Run

Start the API in one terminal. It listens on `http://localhost:4040/`:

```bash
cd ../../http-api
mvn spring-boot:run
```

If the default JDK is older than 25, point `JAVA_HOME` and `PATH` at a JDK 25 first.

Run the benchmark in another terminal, from this directory:

```bash
uv run python benchmark.py --questions 50000 --batch-size 10 --partitions 4
```

Options:

- `--url` API endpoint, default `http://localhost:4040/`
- `--questions` number of questions to generate, default 100
- `--batch-size` questions per HTTP request, default 10. The number of requests per variant is `questions / batch-size`
- `--partitions` number of Spark partitions, default 4
- `--max-in-flight` concurrent requests per partition for the two async variants, default 10
- `--variants` comma-separated subset to run, for example `--variants http-enrich,httpx-partition-async`

## What it does

1. Starts a local Spark session (`local[1]`) and builds a DataFrame with one row per batch, repartitioned and cached.
2. Runs every variant once as a warm-up, so Python worker start-up and connection set-up are not timed.
3. Runs every variant again and times it end to end (`total_ms`, from submitting the job to collecting the results).

## Output

A per-request latency summary per variant (`elapsed_ms`, the HTTP library's own timer: mean, standard deviation, 50th, 90th, 95th and 99th percentile, min, max), followed by the end-to-end `total_ms` per variant.

`total_ms` is the number to compare. `elapsed_ms` measures a single request and says little about throughput, because the async variants overlap many requests.

## Reading the results

- A run is a single measurement. Repeat it before drawing conclusions, and treat differences under about 15% as noise.
- The API sleeps 15 ms per request (`Thread.sleep(15)` in `../../http-api/src/main/java/com/michalklempa/api/Endpoint.java`) to simulate server latency, so a request takes about 17 to 19 ms. Comment the line out and restart the API to measure client and transport overhead alone.
- Spark runs with a single local executor core (`local[1]`), so the sequential variants do not get any parallelism from the partitions. The async variants get their concurrency from `--max-in-flight`.
