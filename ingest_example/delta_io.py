import os
from datetime import datetime
from typing import Optional, Union
import polars as pl
import dagster
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)

IOContext = Union[InputContext, OutputContext]


def map_physical_partitions(
    columns: list[str],
    context: IOContext,
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
        return str(
            datetime.strptime(context.partition_key, "%Y-%m-%d")
            .isocalendar()
            .week
        ).zfill(2)

    time_columns = get_time_columns(context.asset_partitions_def)
    time_values = None

    partition_by = []
    for col in columns:
        if col == "$time":
            time_part_name = time_columns[-1]
            partition_by.append(time_part_name)
            time_values = {time_part_name: context.partition_key}
            continue

        if not col == "$time$expand":
            partition_by.append(col)
            continue

        # $time$expand logic
        partition_by.extend(time_columns)
        # TODO: if this is a multi dimension partition, find time key first
        time_key_parts = context.partition_key.split("-")
        time_values = {
            name: time_key_parts[i] if name != "week" else get_week_value()
            for i, name in enumerate(time_columns)
        }

    return partition_by, time_values


class DeltaIOManager(ConfigurableIOManager):
    tables_root: str
    refresh: bool = False

    def _get_table_path(self, context: IOContext):
        delta_path = context.metadata.get("delta_path")
        if not delta_path:
            raise Exception(
                f"op {context.op_def.name} requires 'delta_path' metadata"
            )
        return os.path.join(self.tables_root, delta_path)

    def handle_output(self, context: OutputContext, df: pl.DataFrame) -> None:
        table_path = self._get_table_path(context)
        write_opts = {}

        mode = "append"
        if self.refresh:
            mode = "overwrite"

        delta_partitions = context.metadata.get("delta_partitions")
        if delta_partitions:
            partition_by, time_values = map_physical_partitions(
                delta_partitions, context
            )

            write_opts["partition_by"] = partition_by

            if time_values:
                if self.refresh:
                    write_opts["partition_filters"] = [
                        (k, "=", v) for k, v in time_values.items()
                    ]
                df = df.with_columns(
                    *[pl.lit(v).alias(k) for k, v in time_values.items()]
                )

        df.write_delta(
            table_path,
            mode=mode,
            delta_write_options=write_opts,
        )

    def load_input(self, context: InputContext) -> pl.DataFrame:
        table_path = self._get_table_path(context)
        read_opts = {}

        delta_partitions = context.metadata.get("delta_partitions")
        if delta_partitions:
            _, time_values = map_physical_partitions(delta_partitions, context)
            read_opts["partitions"] = [
                (k, "=", v) for k, v in time_values.items()
            ]

        return pl.read_delta(table_path, pyarrow_options=read_opts)
