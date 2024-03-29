from dagster import (
    DailyPartitionsDefinition,
)

daily_partitions = DailyPartitionsDefinition(
    start_date="2024-03-20", end_offset=1
)
