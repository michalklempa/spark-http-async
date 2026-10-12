from collections.abc import Callable
from typing import Any

import httpx
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import BinaryType, DoubleType, IntegerType, StringType, StructField, StructType
from spark_async import AsyncMapper, map_partitions_async

BuildRequest = Callable[[Row], httpx.Request]

RESPONSE_FIELDS = [
    StructField("response_body", BinaryType(), True),
    StructField("response_status_code", IntegerType(), True),
    StructField("request_elapsed_ms", DoubleType(), True),
    StructField("response_error", StringType(), True),
]


class HttpSendMapper(AsyncMapper[Row, dict[str, Any]]):
    def __init__(self, max_in_flight: int, build_request: BuildRequest) -> None:
        self.max_in_flight = max_in_flight
        self.build_request = build_request

    async def open(self) -> None:
        self.client = httpx.AsyncClient(http2=False, limits=httpx.Limits(max_connections=self.max_in_flight))

    async def map(self, row: Row) -> dict[str, Any]:
        result: dict[str, Any] = row.asDict(recursive=True)
        try:
            response = await self.client.send(self.build_request(row))
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


def send(requests: DataFrame, build_request: BuildRequest, max_in_flight: int = 10) -> DataFrame:
    schema = StructType(requests.schema.fields + RESPONSE_FIELDS)
    rdd = map_partitions_async(requests.rdd, HttpSendMapper(max_in_flight, build_request), max_in_flight)
    return requests.sparkSession.createDataFrame(rdd, schema)
