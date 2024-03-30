import os
import json

from typing import Optional
from datetime import datetime
from collections import defaultdict

from dagster import (
    sensor,
    SensorEvaluationContext,
    define_asset_job,
    AssetSelection,
    SensorResult,
    SkipReason,
    RunRequest,
)

Cursor = dict[str, set[str]]  # partition -> set of filenames


@sensor(
    job=define_asset_job("ingest_job", selection=AssetSelection.keys("listing"))
)
def ingest_sensor(context: SensorEvaluationContext):
    curr_cursor: Cursor = defaultdict(set)
    curr_time = datetime.now()

    prev_cursor: Cursor = {}
    prev_time = None

    if context.cursor:
        obj = json.loads(context.cursor)
        prev_cursor = {k: set(v) for k, v in obj[0].items()}
        # NOTE: ideally we would use `context.last_tick_start_time` but it doesn't exist
        #   and last_tick_completion_time is (a) too late; (b) exhibiting buggy behaviour
        #   where it returns a time that is before the actual completion time)
        prev_time = datetime.fromtimestamp(obj[1])

    run_requests = []
    days = set([curr_time.strftime("%Y-%m-%d")])
    if prev_time:
        days.add(prev_time.strftime("%Y-%m-%d"))

    for day in days:
        ingest_partition = f"files_to_ingest/day={day}"
        if not os.path.isdir(ingest_partition):
            continue

        for filename in os.listdir(ingest_partition):
            if filename.endswith(".txt"):
                curr_cursor[day].add(filename)

        new_files = curr_cursor[day] - prev_cursor.get(day, set())

        if new_files:
            run_requests.append(
                RunRequest(
                    partition_key=day,
                    run_config={
                        "ops": {
                            "listing": {
                                "config": {"new_files": list(new_files)}
                            }
                        }
                    },
                )
            )

    if not run_requests:
        return SkipReason("No new ingestion files found")

    return SensorResult(
        cursor=json.dumps(
            [
                {k: list(v) for k, v in curr_cursor.items()},
                curr_time.timestamp(),
            ]
        ),
        run_requests=run_requests,
    )
