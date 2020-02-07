/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_CERT_USER_H
#define TIMESCALEDB_CERT_USER_H

#include <postgres.h>
#include <lib/stringinfo.h>

#include "export.h"

/* Length of a MD5 sum, in bytes. There is no constant defined in
 * common/md5.h, all usage is hard-coded, so we add one here. */
#define MD5_SUM_LEN 16

typedef struct UserCertInfo
{
	Oid roleid;
	NameData role_name;
	StringInfoData key_file;
	StringInfoData crt_file;
} UserCertInfo;

extern void ts_cert_info_from_name(Name role_name, UserCertInfo *cert_info);
extern StringInfo ts_user_cert_path(const char *user_name);
extern StringInfo ts_user_key_path(const char *user_name);

#endif /* TIMESCALEDB_CERT_USER_H */
