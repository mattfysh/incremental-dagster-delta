# dagster + delta - incremental compute prototype

* all workloads are incremental by default
* set `resources.delta_io_manager.config.refresh` to refresh a delta asset partition
* the listing asset for files to ingest will refresh if not run by the sensor


# questions

1. why does an asset error if the upstream partition_key hasn't been materialized yet?
    * FileNotFoundError: [Errno 2] No such file or directory: '/Users/matt/Projects/sandbox/ii/dagster_home/storage/listing/2024-03-28'

2. explore concurrency bugs

3. how to put the `refresh` flag on the op config instead
    * and have delta IO manager still know to overwrite instead of append

4. can the asset code logic that checks for `refresh` and `watermark` be abstracted out
    * so the code never has to be aware of incremental vs refresh run
