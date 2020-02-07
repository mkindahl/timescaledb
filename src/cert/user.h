/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_CERT_USER_H
#define TIMESCALEDB_CERT_USER_H

#include <postgres.h>

typedef struct UserCertInfo
{
	Oid roleid;
	char role_name[NAMEDATALEN];
	char key_file[MAXPGPATH];
	char crt_file[MAXPGPATH];
} UserCertInfo;

void ts_cert_info_from_name(Name role_name, UserCertInfo *cert_info);

#endif /* TIMESCALEDB_CERT_USER_H */
