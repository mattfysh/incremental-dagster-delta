import polars as pl
from dagster import (
    asset,
    DagsterEventType,
    AssetExecutionContext,
    ResourceParam,
    EventRecordsFilter,
)
from ..partitions import daily_partitions
from ..delta_io import DeltaIOManager


def get_watermark(context: AssetExecutionContext):
    events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=context.asset_key,
            asset_partitions=[context.partition_key],
        ),
        ascending=False,
    )
    if len(events) == 0:
        return None
    latest_event = events[0].event_log_entry.dagster_event
    metadata = latest_event.event_specific_data.materialization.metadata
    return metadata.get("watermark")


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
        watermark = get_watermark(context)
        if watermark:
            print("Applying watermark value to listing:", watermark.value)
            files_to_process = listing[watermark.value :]

    data = []
    print(f"Porcessing {len(files_to_process)} files...")
    for filename in files_to_process:
        with open(f"files_to_ingest/day={day}/{filename}", "r") as file:
            word = file.read()
        data.append((filename, word, len(word)))

    df = pl.DataFrame(
        data, schema=["filename", "word", "word_length"], orient="row"
    )

    context.add_output_metadata({"watermark": len(listing)})
    return df
