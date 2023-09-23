-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Deprecated partition hash function
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_partition_for_key(val anyelement)
    RETURNS int
    AS '@MODULE_PATHNAME@', 'ts_get_partition_for_key' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_partition_hash(val anyelement)
    RETURNS int
    AS '@MODULE_PATHNAME@', 'ts_get_partition_hash' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Dimension type used in create_hypertable, add_dimension, etc. It is
-- deliberately an opaque type.
CREATE TYPE dimension_info;

CREATE FUNCTION dimension_info_in(cstring) RETURNS dimension_info
AS '@MODULE_PATHNAME@', 'ts_dimension_info_in' LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION dimension_info_out(dimension_info) RETURNS cstring
AS '@MODULE_PATHNAME@', 'ts_dimension_info_in' LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE dimension_info (
    INPUT = dimension_info_in,
    OUTPUT = dimension_info_out,
    INTERNALLENGTH = VARIABLE
);

CREATE OR REPLACE FUNCTION @extschema@.ByHash(column_name NAME, number_partitions INTEGER,
      	  	  	   		    partition_func regproc = NULL)
RETURNS dimension_info LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_hash_dimension';

CREATE OR REPLACE FUNCTION @extschema@.ByRange(column_name NAME,
					     partition_interval ANYELEMENT = NULL::bigint,
       	  	  	   		     partition_func regproc = NULL)
RETURNS dimension_info LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_range_dimension';
