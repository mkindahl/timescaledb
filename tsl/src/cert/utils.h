/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_CERT_UTILS_H
#define TIMESCALEDB_CERT_UTILS_H

#include <postgres.h>
#include <funcapi.h>

#include <openssl/x509.h>

#define UB_TITLE_LENGTH 64
#define UB_SERIAL_NUMBER_LENGTH 64
#define UB_ORGANIZATIONAL_UNIT_NAME_LENGTH 64
#define UB_ORGANIZATIONAL_NAME_LENGTH 64
#define UB_COMMON_NAME_LENGTH 64
#define UB_STATE_NAME_LENGTH 128
#define UB_LOCALITY_LENGTH 128

typedef struct DistinguishedName
{
	char country[2 + 1]; /* Country code plus NULL */
	char organization[UB_ORGANIZATIONAL_NAME_LENGTH + 1];
	char organizational_unit[UB_ORGANIZATIONAL_UNIT_NAME_LENGTH + 1];
	char state[UB_STATE_NAME_LENGTH];
	char common_name[UB_COMMON_NAME_LENGTH + 1];
	char serial_number[UB_SERIAL_NUMBER_LENGTH + 1];
	char locality[UB_LOCALITY_LENGTH + 1];
} DistinguishedName;

#endif /* TIMESCALEDB_CERT_UTILS_H */
