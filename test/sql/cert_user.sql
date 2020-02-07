-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION get_user_cert_info(user_name NAME)
RETURNS TABLE(role_id INTEGER, role_name TEXT, crt_file TEXT, key_file TEXT)
AS :MODULE_PATHNAME, 'ts_pg_get_user_cert_info'
LANGUAGE C VOLATILE;

SELECT setting AS datadir FROM pg_settings WHERE name = 'data_directory' \gset

SELECT role_name
     , REPLACE(crt_file, :'datadir', '<datadir>') AS crt_file
     , REPLACE(key_file, :'datadir', '<datadir>') AS key_file
  FROM get_user_cert_info(:'ROLE_SUPERUSER');

\set ON_ERROR_STOP 0
SELECT * FROM get_user_cert_info('non-existant');
\set ON_ERROR_STOP 1
