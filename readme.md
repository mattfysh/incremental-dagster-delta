# dagster + delta - incremental compute prototype










> refresh flag


> delta -> delta downstream incremental

> downstream


# questions
why does an asset error if the upstream partition_key hasn't been materialized yet?
  FileNotFoundError: [Errno 2] No such file or directory: '/Users/matt/Projects/sandbox/ii/dagster_home/storage/listing/2024-03-28'
explore concurrency bugs


# todo
when mapping partition key to physical partitions for Multi, find time key (split by '|')
