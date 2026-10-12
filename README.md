# spark-http-async

Enrich a PySpark DataFrame with HTTP responses. Every row describes one request (`url`, `headers`, `body`). `enrich()` sends the requests concurrently from each Spark partition, with a bounded number of requests in flight, and returns the same rows with the response attached.

It is built on [spark-async](https://github.com/michalklempa/spark-async), which runs an async function over the rows of each partition. Spark has no built-in async operator for this.

## Requirements

- Python 3.13 or newer
- PySpark 4.2 or newer
- [uv](https://docs.astral.sh/uv/)

Install uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Install

```bash
uv add "spark-http-async @ git+https://github.com/michalklempa/spark-http-async"
```

`spark-async` is installed as a dependency, from its GitHub repository.

## Use it in PySpark

The input DataFrame needs these columns:

| Column | Type | Meaning |
|---|---|---|
| `url` | string | URL to `POST` to |
| `body` | binary, nullable | request body. The column is optional: without it, requests are sent with no body |
| `headers` | map of string to string, nullable | request headers. The column is optional: without it, only the default `httpx` headers are sent |

Any other columns are carried through unchanged.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_http_async import enrich

spark = SparkSession.builder.master("local[*]").getOrCreate()

requests = (
    spark.createDataFrame([("a",), ("b",), ("c",)], ["question_id"])
    .withColumn("url", F.lit("http://localhost:4040/"))
    .withColumn("headers", F.create_map(F.lit("Content-Type"), F.lit("application/json")))
    .withColumn("body", F.to_json(F.array(F.struct("question_id"))).cast("binary"))
)

enriched = enrich(requests, max_in_flight=50)

enriched.select(
    "question_id",
    "response_status_code",
    "request_elapsed_ms",
    F.col("response_body").cast("string").alias("response"),
    "response_error",
).show(truncate=False)
```

`enrich(df, max_in_flight=10)` returns a DataFrame with the input columns plus:

| Column | Type | Meaning |
|---|---|---|
| `response_body` | binary | response body |
| `response_status_code` | int | HTTP status code. A 4xx or 5xx response is a normal result, not an error |
| `request_elapsed_ms` | double | time until the response arrived, as measured by `httpx` |
| `response_error` | string | `ExceptionType: message` when the request failed at the transport level (connection refused, timeout, ...). The other response columns are then null |

`max_in_flight` is the number of requests in flight per partition, so the total is the number of partitions running at once times `max_in_flight`. Keep that in mind when the target service has rate limits.

Rows come back in completion order, not input order. Repartition the input first if the work is unevenly spread: concurrency comes from `max_in_flight` within a partition, and from partitions across the cluster.

## Run it on a Spark cluster

The executors' Python processes must be able to import `spark_http_async`, `spark_async` and `httpx`.

- Install the package in the Python environment of every worker node, or
- ship a packed virtual environment with `spark-submit --archives` and point `PYSPARK_PYTHON` at it.

In local mode, make sure the workers use the same interpreter as the driver:

```python
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
```

## Benchmarks

[benchmarks/](benchmarks/) compares `enrich()` with plain `requests` and `httpx` clients against a small test API. See its README for how to run it.

## Develop

```bash
git clone https://github.com/michalklempa/spark-http-async
cd spark-http-async
uv sync
```

## Build a package

```bash
uv build
```

This writes a wheel and a source archive to `dist/`.

## Publish to PyPI (optional)

Users can install straight from GitHub, so publishing is not required. `spark-async` must be on PyPI before this package can be, because PyPI rejects dependencies that point at a Git URL. Publish `spark-async` first, then replace the `[tool.uv.sources]` entry in `pyproject.toml` and publish this package the same way:

```bash
uv build
uv publish --publish-url https://test.pypi.org/legacy/ --token <testpypi-token>
uv publish --token <pypi-token>
```

A version number can be published only once. Bump `version` in `pyproject.toml` for every release.

## License

MIT, see [LICENSE](LICENSE). The runtime dependencies are `httpx` (BSD-3-Clause), `pyspark` (Apache-2.0) and `spark-async` (MIT), plus their own dependencies, which carry permissive licenses (BSD, MIT, PSF) except `certifi` (MPL-2.0, which applies only to modified copies of its own files).
