import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LIBRARY_ROOT = os.path.dirname(HERE)
sys.path[:0] = [HERE, LIBRARY_ROOT]
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [HERE, LIBRARY_ROOT, os.environ.get("PYTHONPATH")]))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_http_async import enrich
from variants import DEFAULT_MAX_IN_FLIGHT, enrich_rows, prepare_for_enrich, variants


def session():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("spark-http-async-benchmarks")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def batches(spark, count, batch_size, partitions):
    frame = (
        spark.range(count)
        .withColumn("question_id", F.concat(F.lit("question_"), F.col("id").cast("string")))
        .withColumn("batch", F.floor(F.col("id") / batch_size).cast("long"))
        .groupBy("batch")
        .agg(F.collect_list(F.struct("question_id")).alias("questions"))
        .repartition(partitions)
        .cache()
    )
    frame.count()
    return frame


def timed(build, rdd):
    start_ns = time.perf_counter_ns()
    results = build(rdd).collect()
    return results, (time.perf_counter_ns() - start_ns) / 1e6


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:4040/")
    parser.add_argument("--questions", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--max-in-flight", type=int, default=DEFAULT_MAX_IN_FLIGHT)
    parser.add_argument("--variants", help="comma-separated variant names, default all")
    args = parser.parse_args()

    spark = session()
    cached = batches(spark, args.questions, args.batch_size, args.partitions)
    enrich_df = prepare_for_enrich(cached, args.url).cache()
    enrich_df.count()

    builders = variants(args.url, args.max_in_flight, enrich_df, enrich)
    if args.variants:
        selected = args.variants.split(",")
        unknown = [name for name in selected if name not in builders]
        if unknown:
            parser.error(f"unknown variants: {', '.join(unknown)}")
        builders = {name: builders[name] for name in selected}

    for build in builders.values():
        timed(build, cached.rdd)

    results = []
    totals = []
    for name, build in builders.items():
        rows, total_ms = timed(build, cached.rdd)
        results += enrich_rows(rows) if name == "http-enrich" else rows
        totals.append({"library": name, "total_ms": total_ms})

    frame = pd.DataFrame(results, columns=["library", "batch", "status_code", "http_version", "elapsed_ms"])

    pd.set_option("display.width", None)
    print(frame.groupby("library")["elapsed_ms"].describe(percentiles=[.5, .9, .95, .99]))
    print()
    print(pd.DataFrame(totals).set_index("library"))

    spark.stop()


if __name__ == "__main__":
    main()
