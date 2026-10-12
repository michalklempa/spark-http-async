import httpx
from pyspark.sql import DataFrame, Row

from spark_http_async.send import send


def default_build_request(row: Row) -> httpx.Request:
    columns = row.asDict()
    body = columns.get("body")
    headers = columns.get("headers")
    return httpx.Request(
        columns.get("method") or "POST",
        columns["url"],
        content=bytes(body) if body is not None else None,
        headers=dict(headers) if headers else None,
    )


def enrich(df: DataFrame, max_in_flight: int = 10) -> DataFrame:
    return send(df, default_build_request, max_in_flight)
