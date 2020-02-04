/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <openssl/evp.h>

#ifndef TIMESCALEDB_CERT_KEYGEN_H
#define TIMESCALEDB_CERT_KEYGEN_H

EVP_PKEY *ts_cert_generate_key(void);

#endif
