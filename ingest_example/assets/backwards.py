import polars as pl
from dagster import asset
from ..partitions import daily_partitions


@asset(
    partitions_def=daily_partitions,
    io_manager_key="delta_io_manager",
    metadata={
        "delta_path": "backwards/v1",
        "delta_partitions": ["$time$expand"],
    },
)
def backwards(processed: pl.DataFrame):
    return processed.with_columns(pl.col("word").str.reverse().alias("word"))
