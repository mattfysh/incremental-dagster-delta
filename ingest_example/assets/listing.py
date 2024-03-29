import os
from typing import Optional
from dagster import (
    asset,
    Config,
    AssetExecutionContext,
)
from ..partitions import daily_partitions


class ListingConfig(Config):
    new_files: Optional[list[str]] = None


@asset(partitions_def=daily_partitions)
def listing(context: AssetExecutionContext, config: ListingConfig) -> list[str]:
    day = context.partition_key

    if not config.new_files:
        # backfill
        ingest_partition = f"files_to_ingest/day={day}"
        if not os.path.isdir(ingest_partition):
            return []
        return [
            file
            for file in os.listdir(ingest_partition)
            if file.endswith(".txt")
        ]

    # incremental update
    from .. import defs  # avoids circular ref

    try:
        prev = defs.get_asset_value_loader().load_asset_value(
            "listing", partition_key=context.partition_key
        )
    except FileNotFoundError:
        print("No previous materialization found")
        prev = []

    return prev + config.new_files
