import os
from datetime import datetime
from typing import Any, Optional
import polars as pl
import dagster
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)


def map_physical_partitions(
    columns: list[str],
    p_def: dagster.PartitionsDefinition,
    p_key: str,
):
    def get_time_columns(
        p_def: dagster.PartitionsDefinition,
    ) -> Optional[list[str]]:
        if isinstance(p_def, dagster.MonthlyPartitionsDefinition):
            return ["year", "month"]
        elif isinstance(p_def, dagster.DailyPartitionsDefinition):
            return ["year", "month", "day"]
        elif isinstance(p_def, dagster.HourlyPartitionsDefinition):
            return ["year", "month", "day", "hour"]
        elif isinstance(p_def, dagster.WeeklyPartitionsDefinition):
            return ["year", "week"]
        elif (
            isinstance(p_def, dagster.MultiPartitionsDefinition)
            and p_def.has_time_window_dimension
        ):
            return get_time_columns(p_def.time_window_partitions_def)
        else:
            return None

    def get_week_value():
        week = datetime.strptime(p_key, "%Y-%m-%d").isocalendar().week
        return str(week).zfill(2)

    time_columns = get_time_columns(p_def)
    time_values = None

    partition_by = []
    for col in columns:
        if col == "$time":
            time_part_name = time_columns[-1]
            partition_by.append(time_part_name)
            time_values = {time_part_name: p_key}
            continue

        if not col == "$time$expand":
            partition_by.append(col)
            continue

        # $time$expand logic
        partition_by.extend(time_columns)
        # TODO: if this is a multi dimension partition, find time key first
        time_key_parts = p_key.split("-")
        time_values = {
            name: time_key_parts[i] if name != "week" else get_week_value()
            for i, name in enumerate(time_columns)
        }

    return partition_by, time_values


class DeltaIOManager(ConfigurableIOManager):
    tables_root: str

    def handle_output(self, context: OutputContext, df: pl.DataFrame) -> None:
        delta_path = context.metadata.get("delta_path")
        if not delta_path:
            raise Exception(
                f"op {context.op_def.name} requires 'delta_path' metadata"
            )
        table_path = os.path.join(self.tables_root, delta_path)
        write_opts = {}

        delta_partitions = context.metadata.get("delta_partitions")
        if delta_partitions:
            partition_by, time_values = map_physical_partitions(
                delta_partitions,
                context.asset_partitions_def,
                context.partition_key,
            )

            write_opts["partition_by"] = partition_by
            if time_values:
                df = df.with_columns(
                    *[pl.lit(v).alias(k) for k, v in time_values.items()]
                )

        mode = "append"
        if context.config.get("full_refresh"):
            mode = "overwrite"

        df.write_delta(
            table_path,
            mode="append",
            delta_write_options=write_opts,
        )

    def load_input(self, context: InputContext) -> Any:
        raise Exception("DELTAIO: load_input todo")
