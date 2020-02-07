/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "cert/user.h"

#include <postgres.h>

#include <access/htup_details.h>
#include <common/md5.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <utils/acl.h>
#include <utils/builtins.h>

#include "guc.h"
#include "extension_constants.h"

/* Length of a MD5 sum, in bytes.
 *
 * There is no constant defined in common/md5.h, all usage is hard-coded. */
#define MD5_SUM_LEN 16

typedef enum PathKind
{
	PATH_KIND_CRT,
	PATH_KIND_KEY
} PathKind;

/* Path description for human consumption */
static const char *path_kind_text[PATH_KIND_KEY + 1] = {
	[PATH_KIND_CRT] = "certificate",
	[PATH_KIND_KEY] = "private key",
};

/* Path extension string for file system */
static const char *path_kind_ext[PATH_KIND_KEY + 1] = {
	[PATH_KIND_CRT] = "crt",
	[PATH_KIND_KEY] = "key",
};

/*
 * Helper function to report error.
 *
 * This is needed to avoid code coverage reporting low coverage for error
 * cases in `make_user_cert_path` that cannot be reached in normal situations.
 */
static void
report_path_error(PathKind path_kind, const char *user_name)
{
	elog(ERROR,
		 "cannot write %s for user \"%s\": path too long",
		 path_kind_text[path_kind],
		 user_name);
}

/*
 * Make a user path with the given extension and user name in a portable and
 * safe manner.
 *
 * We use MD5 to compute a filename for the user name, which allow all forms
 * of user names. It is not necessary for the function to be cryptographically
 * secure, only to have a low risk of collisions, and MD5 is fast and with a
 * low risk of collisions.
 *
 * Will return the resulting path, or abort with an error.
 */
static void
make_user_cert_path(char *ret_path, const char *user_name, PathKind path_kind)
{
	const char *ssl_dir = ts_guc_ssl_dir ? ts_guc_ssl_dir : DataDir;
	/* 2 characters per byte, plus terminating NUL character */
	char hexsum[2 * MD5_SUM_LEN + 1];

	pg_md5_hash(user_name, strlen(user_name), hexsum);

	if (strlcpy(ret_path, ssl_dir, MAXPGPATH) > MAXPGPATH)
		report_path_error(path_kind, user_name);

	canonicalize_path(ret_path);

	join_path_components(ret_path, ret_path, EXTENSION_NAME);
	join_path_components(ret_path, ret_path, "certs");
	join_path_components(ret_path, ret_path, hexsum);

	if ((strlcat(ret_path, ".", MAXPGPATH) > MAXPGPATH) ||
		(strlcat(ret_path, path_kind_ext[path_kind], MAXPGPATH) > MAXPGPATH))
		report_path_error(path_kind, user_name);
}

void
ts_cert_info_from_name(Name user_name, UserCertInfo *result)
{
	result->roleid = get_role_oid(user_name->data, false);
	if (strlcpy(result->role_name, user_name->data, NAMEDATALEN) > NAMEDATALEN)
		elog(ERROR, "user name \"%s\" too long", user_name->data);
	make_user_cert_path(result->crt_file, user_name->data, PATH_KIND_CRT);
	make_user_cert_path(result->key_file, user_name->data, PATH_KIND_KEY);
}

TS_FUNCTION_INFO_V1(ts_pg_get_user_cert_info);

Datum
ts_pg_get_user_cert_info(PG_FUNCTION_ARGS)
{
	Name username = PG_GETARG_NAME(0);
	UserCertInfo cert_info;
	TupleDesc tupdesc;
	Datum values[4];
	bool nulls[4] = { false };
	HeapTuple tuple;

	ts_cert_info_from_name(username, &cert_info);
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[0] = ObjectIdGetDatum(cert_info.roleid);
	values[1] = CStringGetTextDatum(cert_info.role_name);
	values[2] = CStringGetTextDatum(cert_info.key_file);
	values[3] = CStringGetTextDatum(cert_info.crt_file);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}
