-- API changes related to hypertable generalization
CREATE OR REPLACE FUNCTION timescaledb_experimental.create_hypertable(
    relation                REGCLASS,
    partition_column        NAME,
    partition_interval      ANYELEMENT = NULL::BIGINT,
    partition_func          REGPROC = NULL,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    migrate_data            BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create_general' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION timescaledb_experimental.add_dimension(
    hypertable              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    partition_interval      ANYELEMENT = NULL::BIGINT,
    partition_func          REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add_general' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION timescaledb_experimental.set_partitioning_interval(
    hypertable              REGCLASS,
    partition_interval      ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;
