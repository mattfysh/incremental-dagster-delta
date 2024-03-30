export DAGSTER_HOME="$PWD/dagster_home"

# rm -rf $DAGSTER_HOME delta_tables
mkdir -p $DAGSTER_HOME

dagster dev
