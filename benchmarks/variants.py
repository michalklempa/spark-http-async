import asyncio

import httpx
import requests
from pyspark.sql import functions as F

DEFAULT_MAX_IN_FLIGHT = 10


def payload(row):
    return [question.asDict() for question in row.questions]


def post_requests(url, row):
    response = requests.post(url, json=payload(row))
    return "requests", row.batch, response.status_code, "1.1", response.elapsed.total_seconds() * 1000


def post_partition_requests(url, rows):
    for row in rows:
        response = requests.post(url, json=payload(row))
        yield "requests-partition", row.batch, response.status_code, "1.1", response.elapsed.total_seconds() * 1000


def post_partition_httpx(url, rows):
    with httpx.Client() as client:
        for row in rows:
            response = client.post(url, json=payload(row))
            yield "httpx-partition", row.batch, response.status_code, response.http_version, response.elapsed.total_seconds() * 1000


async def post_async(client, url, row, semaphore):
    async with semaphore:
        response = await client.post(url, json=payload(row))
        return "httpx-partition-async", row.batch, response.status_code, response.http_version, response.elapsed.total_seconds() * 1000


def post_partition_async(url, rows, max_in_flight):
    rows = list(rows)

    async def run():
        semaphore = asyncio.Semaphore(max_in_flight)
        limits = httpx.Limits(max_connections=max_in_flight)
        async with httpx.AsyncClient(limits=limits) as client:
            return await asyncio.gather(*(post_async(client, url, row, semaphore) for row in rows))

    return asyncio.run(run())


def prepare_for_enrich(df, url):
    return (
        df
        .withColumn("url", F.lit(url))
        .withColumn("headers", F.create_map(F.lit("Content-Type"), F.lit("application/json")))
        .withColumn("body", F.to_json(F.col("questions")).cast("binary"))
    )


def enrich_rows(rows):
    return [
        ("http-enrich", row.batch, row.response_status_code, "1.1", row.request_elapsed_ms)
        for row in rows
    ]


def variants(url, max_in_flight, enrich_df, enrich):
    return {
        "requests": lambda source: source.map(lambda row: post_requests(url, row)),
        "requests-partition": lambda source: source.mapPartitions(lambda rows: post_partition_requests(url, rows)),
        "httpx-partition": lambda source: source.mapPartitions(lambda rows: post_partition_httpx(url, rows)),
        "httpx-partition-async": lambda source: source.mapPartitions(
            lambda rows: post_partition_async(url, rows, max_in_flight)),
        "http-enrich": lambda source: enrich(enrich_df, max_in_flight),
    }
