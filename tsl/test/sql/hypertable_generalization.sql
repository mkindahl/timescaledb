-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Validate generalized hypertable for smallint
CREATE TABLE test_table_smallint(id SMALLINT, device INTEGER, time TIMESTAMPTZ);
SELECT create_hypertable('test_table_smallint', ByRange('id'));

-- default interval
SELECT integer_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'test_table_smallint';

-- Add data with default partition (10000)
INSERT INTO test_table_smallint VALUES (1, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_smallint VALUES (9999, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_smallint VALUES (10000, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_smallint VALUES (20000, 10, '01-01-2023 11:00'::TIMESTAMPTZ);

-- Number of chunks
SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='test_table_smallint';

-- Validate generalized hypertable for int
CREATE TABLE test_table_int(id INTEGER, device INTEGER, time TIMESTAMPTZ);
SELECT create_hypertable('test_table_int', ByRange('id'));

-- Default interval
SELECT integer_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'test_table_int';

-- Add data
INSERT INTO test_table_int VALUES (1, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_int VALUES (99999, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_int VALUES (100000, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_int VALUES (200000, 10, '01-01-2023 11:00'::TIMESTAMPTZ);

-- Number of chunks
SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='test_table_int';

-- Validate generalized hypertable for bigint
CREATE TABLE test_table_bigint(id BIGINT, device INTEGER, time TIMESTAMPTZ);
SELECT create_hypertable('test_table_bigint', ByRange('id'));

-- Default interval
SELECT integer_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'test_table_bigint';

-- Add data
INSERT INTO test_table_bigint VALUES (1, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_bigint VALUES (999999, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_bigint VALUES (1000000, 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_bigint VALUES (2000000, 10, '01-01-2023 11:00'::TIMESTAMPTZ);

-- Number of chunks
SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='test_table_bigint';

DROP TABLE test_table_smallint;
DROP TABLE test_table_int;
DROP TABLE test_table_bigint;

-- Create hypertable with SERIAL column
CREATE TABLE jobs_serial (job_id SERIAL, device_id INTEGER, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ, PRIMARY KEY (job_id));
SELECT create_hypertable('jobs_serial', ByRange('job_id', partition_interval => 30));

-- Insert data
INSERT INTO jobs_serial (device_id, start_time, end_time)
SELECT abs(timestamp_hash(t::timestamp)) % 10, t, t + INTERVAL '1 day'
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00':: TIMESTAMPTZ,'1 hour')t;

-- Verify chunk pruning
EXPLAIN VERBOSE SELECT * FROM jobs_serial WHERE job_id < 30;
EXPLAIN VERBOSE SELECT * FROM jobs_serial WHERE job_id >= 30 AND job_id < 90;
EXPLAIN VERBOSE SELECT * FROM jobs_serial WHERE job_id > 90;

-- Update rows
UPDATE jobs_serial SET end_time = end_time + INTERVAL '1 hour' where job_id = 1;
UPDATE jobs_serial SET end_time = end_time + INTERVAL '1 hour' where job_id = 30;
UPDATE jobs_serial SET end_time = end_time + INTERVAL '1 hour' where job_id = 90;

SELECT start_time, end_time FROM jobs_serial WHERE job_id = 1;
SELECT start_time, end_time FROM jobs_serial WHERE job_id = 30;
SELECT start_time, end_time FROM jobs_serial WHERE job_id = 90;

-- Delete rows
DELETE FROM jobs_serial WHERE job_id < 10;

SELECT count(*) FROM jobs_serial WHERE job_id < 30;

DROP TABLE jobs_serial;

-- Create and validate hypertable with BIGSERIAL column
CREATE TABLE jobs_big_serial (job_id BIGSERIAL, device_id INTEGER, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ, PRIMARY KEY (job_id));
SELECT create_hypertable('jobs_big_serial', ByRange('job_id', 100));

-- Insert data
INSERT INTO jobs_big_serial (device_id, start_time, end_time)
SELECT abs(timestamp_hash(t::timestamp)) % 10, t, t + INTERVAL '1 day'
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00'::TIMESTAMPTZ,'30 mins')t;

-- Verify #chunks
SELECT count(*) FROM timescaledb_information.chunks;

-- Get current sequence and verify updating sequence
SELECT currval(pg_get_serial_sequence('jobs_big_serial', 'job_id'));

-- Update sequence value to 500
SELECT setval(pg_get_serial_sequence('jobs_big_serial', 'job_id'), 500, false);

-- Insert few rows and verify that the next sequence starts from 500
INSERT INTO jobs_big_serial (device_id, start_time, end_time)
SELECT abs(timestamp_hash(t::timestamp)) % 10, t, t + INTERVAL '1 day'
FROM generate_series('2018-03-09 1:00'::TIMESTAMPTZ, '2018-03-10 1:00'::TIMESTAMPTZ,'30 mins')t;

-- No data should exist for job_id >= 290 to job_id < 500
SELECT count(*) FROM jobs_big_serial WHERE job_id >= 290 AND job_id < 500;

-- The new rows should be added with job_id > 500
SELECT count(*) from jobs_big_serial WHERE job_id > 500;

-- Verify show_chunks API
SELECT show_chunks('jobs_big_serial', older_than => 100);
SELECT show_chunks('jobs_big_serial', newer_than => 200, older_than => 300);
SELECT show_chunks('jobs_big_serial', newer_than => 500);

-- Verify drop_chunks API
SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name =
'jobs_big_serial';

SELECT drop_chunks('jobs_big_serial', newer_than => 500);
SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'jobs_big_serial';

SELECT drop_chunks('jobs_big_serial', newer_than => 200, older_than => 300);
SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'jobs_big_serial';

DROP TABLE jobs_big_serial;

-- Verify partition function
CREATE OR REPLACE FUNCTION part_func(id TEXT)
	RETURNS INTEGER LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
DECLARE
	retval INTEGER;
BEGIN
	retval := CAST(id AS INTEGER);
	RETURN retval;
END
$BODY$;

CREATE TABLE test_table_int(id TEXT, device INTEGER, time TIMESTAMPTZ);
SELECT create_hypertable('test_table_int', ByRange('id', 10, partition_func => 'part_func'));

INSERT INTO test_table_int VALUES('1', 1, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_int VALUES('10', 10, '01-01-2023 11:00'::TIMESTAMPTZ);
INSERT INTO test_table_int VALUES('29', 100, '01-01-2023 11:00'::TIMESTAMPTZ);

SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'test_table_int';

DROP TABLE test_table_int;
DROP FUNCTION part_func;

-- Migrate data
CREATE TABLE test_table_int(id INTEGER, device INTEGER, time TIMESTAMPTZ);
INSERT INTO test_table_int SELECT t, t%10, '01-01-2023 11:00'::TIMESTAMPTZ FROM generate_series(1, 50, 1) t;

SELECT create_hypertable('test_table_int', ByRange('id', 10), migrate_data => true);

SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'test_table_int';

DROP TABLE test_table_int;

-- Create default indexes
CREATE TABLE test_table_int(id INTEGER, device INTEGER, time TIMESTAMPTZ);

SELECT create_hypertable('test_table_int', ByRange('id', 10), create_default_indexes => false);

SELECT indexname FROM pg_indexes WHERE tablename = 'test_table_int';

DROP TABLE test_table_int;

-- if_not_exists
CREATE TABLE test_table_int(id INTEGER, device INTEGER, time TIMESTAMPTZ);

SELECT create_hypertable('test_table_int', ByRange('id', 10));

-- No error when if_not_exists => true
SELECT create_hypertable('test_table_int', ByRange('id', 10), if_not_exists => true);
SELECT * FROM _timescaledb_functions.get_create_command('test_table_int');

-- Should throw an error when if_not_exists is not set
\set ON_ERROR_STOP 0
SELECT create_hypertable('test_table_int', ByRange('id', 10));
\set ON_ERROR_STOP 1

DROP TABLE test_table_int;

-- Add dimension
CREATE TABLE test_table_int(id INTEGER, device INTEGER, time TIMESTAMPTZ);
SELECT create_hypertable('test_table_int', ByRange('id', 10), migrate_data => true);

INSERT INTO test_table_int SELECT t, t%10, '01-01-2023 11:00'::TIMESTAMPTZ FROM generate_series(1, 50, 1) t;

SELECT add_dimension('test_table_int', ByRange('device', partition_interval => 2));

SELECT hypertable_name, dimension_number, column_name FROM timescaledb_information.dimensions WHERE hypertable_name = 'test_table_int';

SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='test_table_int';

-- set_partitioning_interval
SELECT set_partitioning_interval('test_table_int', 5, 'device');

SELECT integer_interval FROM timescaledb_information.dimensions WHERE column_name='device';

DROP TABLE test_table_int;

-- Hypertable with time dimension using new API
CREATE TABLE test_time(time TIMESTAMP NOT NULL, device INT, temp FLOAT);
SELECT create_hypertable('test_time', ByRange('time'));

-- Default interval
SELECT time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'test_time';

INSERT INTO test_time SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;

SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='test_time';

SELECT add_dimension('test_time', ByRange('device', partition_interval => 2));

SELECT hypertable_name, dimension_number, column_name FROM timescaledb_information.dimensions WHERE hypertable_name = 'test_time';

SELECT set_partitioning_interval('test_time', INTERVAL '1 day', 'time');

SELECT time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = 'test_time' AND column_name = 'time';

DROP TABLE test_time;
