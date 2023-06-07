-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

------------------------------------------------------------------------
-- Experimental DDL functions and APIs.
--
-- Users should not rely on these functions unless they accept that
-- they can change and/or be removed at any time.
------------------------------------------------------------------------

-- Block new chunk creation on a data node for a distributed
-- hypertable. NULL hypertable means it will block chunks for all
-- distributed hypertables
CREATE OR REPLACE FUNCTION timescaledb_experimental.block_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL, force BOOLEAN = FALSE) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_block_new_chunks' LANGUAGE C VOLATILE;

-- Allow chunk creation on a blocked data node for a distributed
-- hypertable. NULL hypertable means it will allow chunks for all
-- distributed hypertables
CREATE OR REPLACE FUNCTION timescaledb_experimental.allow_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_allow_new_chunks' LANGUAGE C VOLATILE;

CREATE OR REPLACE PROCEDURE timescaledb_experimental.move_chunk(
    chunk REGCLASS,
    source_node NAME = NULL,
    destination_node NAME = NULL,
    operation_id NAME = NULL)
AS '@MODULE_PATHNAME@', 'ts_move_chunk_proc' LANGUAGE C;

CREATE OR REPLACE PROCEDURE timescaledb_experimental.copy_chunk(
    chunk REGCLASS,
    source_node NAME = NULL,
    destination_node NAME = NULL,
    operation_id NAME = NULL)
AS '@MODULE_PATHNAME@', 'ts_copy_chunk_proc' LANGUAGE C;

CREATE OR REPLACE FUNCTION timescaledb_experimental.subscription_exec(
    subscription_command TEXT
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_subscription_exec' LANGUAGE C VOLATILE;

-- A copy_chunk or move_chunk procedure call involves multiple nodes and
-- depending on the data size can take a long time. Failures are possible
-- when this long running activity is ongoing. We need to be able to recover
-- and cleanup such failed chunk copy/move activities and it's done via this
-- procedure
CREATE OR REPLACE PROCEDURE timescaledb_experimental.cleanup_copy_chunk_operation(
    operation_id NAME)
AS '@MODULE_PATHNAME@', 'ts_copy_chunk_cleanup_proc' LANGUAGE C;

-- A generalized hypertable creation API that can be used to convert a PostgreSQL table
-- with TIME/SERIAL/BIGSERIAL columns to a hypertable.
--
-- relation - The OID of the table to be converted
-- partition_column - Name of the partition column
-- partition_interval (Optional) Initial interval for the chunks
-- partition_func (Optional) Partitioning function to be used for partition column
-- create_default_indexes (Optional) Whether or not to create the default indexes
-- if_not_exists (Optional) Do not fail if table is already a hypertable
-- migrate_data (Optional) Set to true to migrate any existing data in the table to chunks
CREATE OR REPLACE FUNCTION timescaledb_experimental.create_hypertable(
    relation                REGCLASS,
    partition_column        NAME,
    partition_interval      ANYELEMENT = NULL::BIGINT,
    partition_func          REGPROC = NULL,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    migrate_data            BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create_general' LANGUAGE C VOLATILE;

-- Add a dimension (of partitioning) to a hypertable
--
-- hypertable - OID of the table to add a dimension to
-- column_name - NAME of the column to use in partitioning for this dimension
-- number_partitions - Number of partitions, for closed dimensions
-- partition_interval - Size of intervals for open dimensions (can be integral or INTERVAL)
-- partition_func - Function used to partition the column
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION timescaledb_experimental.add_dimension(
    hypertable              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    partition_interval      ANYELEMENT = NULL::BIGINT,
    partition_func          REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add_general' LANGUAGE C VOLATILE;

-- Update partition_interval for a hypertable.
--
-- hypertable - The OID of the table corresponding to a hypertable whose
--     partition interval should be updated
-- partition_interval - The new interval. For hypertables with integral/serial/bigserial
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION timescaledb_experimental.set_partitioning_interval(
    hypertable              REGCLASS,
    partition_interval      ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;
