from dagster import Definitions, load_assets_from_modules

from .assets import listing, processed
from .sensors import ingest_sensor
from .delta_io import DeltaIOManager

defs = Definitions(
    assets=load_assets_from_modules([listing, processed]),
    sensors=[ingest_sensor],
    resources={
        "delta_io_manager": DeltaIOManager(
            tables_root="delta_tables", nest_time_partition=True
        )
    },
)
