import polars as pl
from dagster import asset, AssetKey, AssetExecutionContext, ResourceParam
from ..partitions import daily_partitions
from ..delta_io import DeltaIOManager


@asset(
    partitions_def=daily_partitions,
    io_manager_key="delta_io_manager",
    metadata={
        "delta_path": "processed/v1",
        "delta_partitions": ["$time$expand", "word_length"],
    },
)
def processed(
    context: AssetExecutionContext,
    listing: list[str],
    delta_io_manager: ResourceParam[DeltaIOManager],
) -> pl.DataFrame:
    day = context.partition_key

    files_to_process = listing
    if not delta_io_manager.refresh:
        last = context.instance.get_latest_materialization_event(
            AssetKey(["processed", day])
        )
        if last:
            watermark = last.asset_materialization.metadata.get("watermark")
            if watermark:
                files_to_process = listing[watermark.value :]

    data = []
    for filename in files_to_process:
        with open(f"files_to_ingest/day={day}/{filename}", "r") as file:
            word = file.read()
        data.append((filename, word, len(word)))

    df = pl.DataFrame(
        data, schema=["filename", "word", "word_length"], orient="row"
    )

    context.add_output_metadata({"watermark": len(listing)})
    return df
