-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION extract_certificate_issuer(certificate TEXT)
RETURNS TABLE(country TEXT, organization TEXT, organizational_unit TEXT, state TEXT,
	      common_name TEXT, serial_number TEXT, locality TEXT)
AS :TSL_MODULE_PATHNAME, 'ts_pg_extract_certificate_issuer'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION extract_certificate_subject(certificate TEXT)
RETURNS TABLE(country TEXT, organization TEXT, organizational_unit TEXT, state TEXT,
	      common_name TEXT, serial_number TEXT, locality TEXT)
AS :TSL_MODULE_PATHNAME, 'ts_pg_extract_certificate_subject'
LANGUAGE C VOLATILE;

\x on

-- Issuer:
--   C = US, ST = New York, L = New York,
--   O = Timescale, OU = Development, CN = root.timescale.com
-- Subject:
--   C = US, ST = New York,
--   O = Timescale, OU = Development, CN = access.timescale.com
SELECT $$
-----BEGIN CERTIFICATE-----
MIICwDCCAimgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwejELMAkGA1UEBhMCVVMx
ETAPBgNVBAgMCE5ldyBZb3JrMREwDwYDVQQHDAhOZXcgWW9yazESMBAGA1UECgwJ
VGltZXNjYWxlMRQwEgYDVQQLDAtEZXZlbG9wbWVudDEbMBkGA1UEAwwScm9vdC50
aW1lc2NhbGUuY29tMB4XDTIwMDEyOTA3MjYyNloXDTMwMDEyNjA3MjYyNlowaTEL
MAkGA1UEBhMCVVMxETAPBgNVBAgMCE5ldyBZb3JrMRIwEAYDVQQKDAlUaW1lc2Nh
bGUxFDASBgNVBAsMC0RldmVsb3BtZW50MR0wGwYDVQQDDBRhY2Nlc3MudGltZXNj
YWxlLmNvbTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA3U+1U+WpCvIi+qi9
x2b+C6rUMQ2OAkGq3TBd9T6ilQCywRlYCikO4HZeXLPofifDNTkF2ngu420eBO0e
q141KWXxdmjAoRQZAroqNMEysPJda4yfX+d54ie7VcX0MtG02cthj6RrWjUPUNKn
MNXZBVeeClgPATH2c49YX1IKtOsCAwEAAaNmMGQwHQYDVR0OBBYEFEtuDbYLrqky
1r5zwSrSG+B9Ju8rMB8GA1UdIwQYMBaAFKEAN0+5suTST0cRV1uQ87Z5r0tpMBIG
A1UdEwEB/wQIMAYBAf8CAQAwDgYDVR0PAQH/BAQDAgGGMA0GCSqGSIb3DQEBCwUA
A4GBAEhhz3QoPRE/6uHmF0LqVhV76Xze1/ZcEpyl0cb17xan8OPglV1v38z7DvGD
yDKLbuP5rE8mz5jGAHkUSdh66FTVOmtTkcdwWoNBXhuCkacPC3PnR43UMwzQ+ZZu
ps1M7yVhmDbB5ecLLlz5Cv41TZ761M5bUK2hWmTL4ZpNI4nu
-----END CERTIFICATE-----
$$ AS certificate \gset

SELECT * FROM extract_certificate_issuer(:'certificate');
SELECT * FROM extract_certificate_subject(:'certificate');
