/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Utilities for working with certificates, keys, and certificate signing
 * requests.
 *
 * Right now, only Postgres functions for reading keys in PKCS#8 format is
 * provided, but more functions will be added and exported further down the
 * implementation of the certificate support.
 */
#include "cert/utils.h"

#include <postgres.h>
#include <fmgr.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <string.h>

#include <access/htup_details.h>
#include <funcapi.h>
#include <utils/builtins.h>

#include "export.h"

static const char *
copy_attr(const char *start, char *buf, size_t size)
{
	const char *const end = strchr(start, '/');
	const size_t attr_len = end ? end - start : strlen(start);
	memcpy(buf, start, size < attr_len ? size : attr_len);
	return start + attr_len;
}

static void
extract_name_info(const char *str, DistinguishedName *result)
{
	while (*str)
	{
		const char *ptr = str + 1;
		if (ptr[0] == 'C' && ptr[1] == 'N' && ptr[2] == '=')
		{
			str = copy_attr(ptr + 3, result->common_name, sizeof(result->common_name));
			continue;
		}
		if (ptr[0] == 'S' && ptr[1] == 'T' && ptr[2] == '=')
		{
			str = copy_attr(ptr + 3, result->state, sizeof(result->state));
			continue;
		}
		if (ptr[0] == 'L' && ptr[1] == '=')
		{
			str = copy_attr(ptr + 2, result->locality, sizeof(result->locality));
			continue;
		}
		if (ptr[0] == 'O' && ptr[1] == 'U' && ptr[2] == '=')
		{
			str = copy_attr(ptr + 3,
							result->organizational_unit,
							sizeof(result->organizational_unit));
			continue;
		}
		if (ptr[0] == 'C' && ptr[1] == '=')
		{
			str = copy_attr(ptr + 2, result->country, sizeof(result->country));
			continue;
		}
		if (ptr[0] == 'O' && ptr[1] == '=')
		{
			str = copy_attr(ptr + 2, result->organization, sizeof(result->organization));
			continue;
		}
	}
}

static Datum
fill_name_info(const DistinguishedName *name_info, TupleDesc tupdesc)
{
	Datum values[7] = { 0 };
	bool nulls[7];

	memset(nulls, true, sizeof(nulls) / sizeof(*nulls));

	tupdesc = BlessTupleDesc(tupdesc);
	if (*name_info->country)
	{
		values[0] = CStringGetTextDatum(name_info->country);
		nulls[0] = false;
	}

	if (*name_info->organization)
	{
		values[1] = CStringGetTextDatum(name_info->organization);
		nulls[1] = false;
	}

	if (*name_info->organizational_unit)
	{
		values[2] = CStringGetTextDatum(name_info->organizational_unit);
		nulls[2] = false;
	}

	if (*name_info->state)
	{
		values[3] = CStringGetTextDatum(name_info->state);
		nulls[3] = false;
	}

	if (*name_info->common_name)
	{
		values[4] = CStringGetTextDatum(name_info->common_name);
		nulls[4] = false;
	}

	if (*name_info->serial_number)
	{
		values[5] = CStringGetTextDatum(name_info->serial_number);
		nulls[5] = false;
	}

	if (*name_info->locality)
	{
		values[6] = CStringGetTextDatum(name_info->locality);
		nulls[6] = false;
	}

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

static void
pg_attribute_noreturn() report_error(BIO *bio, const char *msg)
{
	BIO_free(bio);
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("%s", msg),
			 errdetail("Error: %s", ERR_reason_error_string(ERR_get_error()))));
}

TS_FUNCTION_INFO_V1(ts_pg_extract_certificate_issuer);
TS_FUNCTION_INFO_V1(ts_pg_extract_certificate_subject);

static Datum
pg_extract_certificate_name(PG_FUNCTION_ARGS, X509_NAME *(*get_name)(const X509 *) )
{
	text *const cert_buf = PG_GETARG_TEXT_PP(0);
	const char *ptr = VARDATA_ANY(cert_buf);
	size_t key_len = VARSIZE_ANY_EXHDR(cert_buf);
	BIO *bio = BIO_new(BIO_s_mem());
	X509 *x509;
	const char *str;
	DistinguishedName name_info = { 0 };
	TupleDesc tupdesc;

	if (BIO_write(bio, ptr, key_len) != key_len)
		report_error(bio, "BIO_write");

	x509 = PEM_read_bio_X509_AUX(bio, NULL, NULL, NULL);
	if (!x509)
		report_error(bio, "PEM_read_bio_X509_AUX");

	str = X509_NAME_oneline(get_name(x509), NULL, 0);
	if (!str)
		report_error(bio, "X509_NAME_oneline");

	extract_name_info(str, &name_info);

	BIO_free(bio);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	return fill_name_info(&name_info, tupdesc);
}

/*
 * Extract distinguished name fields of issuer from a certificate.
 */
Datum
ts_pg_extract_certificate_issuer(PG_FUNCTION_ARGS)
{
	return pg_extract_certificate_name(fcinfo, X509_get_issuer_name);
}

/*
 * Extract distinguished name fields of issuer from a certificate.
 */
Datum
ts_pg_extract_certificate_subject(PG_FUNCTION_ARGS)
{
	return pg_extract_certificate_name(fcinfo, X509_get_subject_name);
}
