from typing import Any

import httpx
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import BinaryType, DoubleType, IntegerType, StringType, StructField, StructType
from spark_async import AsyncMapper, map_partitions_async

RESPONSE_FIELDS = [
    StructField("response_body", BinaryType(), True),
    StructField("response_status_code", IntegerType(), True),
    StructField("request_elapsed_ms", DoubleType(), True),
    StructField("response_error", StringType(), True),
]


class HttpEnrichMapper(AsyncMapper[Row, dict[str, Any]]):
    def __init__(self, max_in_flight: int) -> None:
        self.max_in_flight = max_in_flight

    async def open(self) -> None:
        self.client = httpx.AsyncClient(http2=False, limits=httpx.Limits(max_connections=self.max_in_flight))

    async def map(self, row: Row) -> dict[str, Any]:
        result: dict[str, Any] = row.asDict(recursive=True)
        try:
            response = await self.client.post(
                row.url,
                content=bytes(row.body) if row.body is not None else None,
                headers=dict(row.headers) if row.headers else None,
            )
            result.update(
                response_body=response.content,
                response_status_code=response.status_code,
                request_elapsed_ms=response.elapsed.total_seconds() * 1000,
            )
        except httpx.HTTPError as error:
            result["response_error"] = f"{type(error).__name__}: {error}"
        return result

    async def close(self) -> None:
        await self.client.aclose()


def enrich(df: DataFrame, max_in_flight: int = 10) -> DataFrame:
    schema = StructType(df.schema.fields + RESPONSE_FIELDS)
    rdd = map_partitions_async(df.rdd, HttpEnrichMapper(max_in_flight), max_in_flight)
    return df.sparkSession.createDataFrame(rdd, schema)
