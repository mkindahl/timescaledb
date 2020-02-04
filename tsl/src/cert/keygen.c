/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "keygen.h"

#include <postgres.h>
#include <fmgr.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include "export.h"

static void
pg_attribute_noreturn() report_key_generate_error(EVP_PKEY_CTX *ctx, const char *msg)
{
	EVP_PKEY_CTX_free(ctx);
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("%s", msg),
			 errdetail("Error: %s", ERR_reason_error_string(ERR_get_error()))));
}

/*
 * Generate an SSL key.
 *
 * This will generate an SSL key for either a user or an instance. We have
 * hardcoded both the algorithm and the number of key bits.
 */
EVP_PKEY *
ts_cert_generate_key()
{
	EVP_PKEY *pkey = NULL;
	EVP_PKEY_CTX *ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);

	if (!ctx)
		report_key_generate_error(ctx, "unable to allocate key context");
	if (EVP_PKEY_keygen_init(ctx) <= 0)
		report_key_generate_error(ctx, "unable to initialize key generation context");
	if (EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) <= 0)
		report_key_generate_error(ctx, "unable to set key bits");

	/* Generate key */
	if (EVP_PKEY_keygen(ctx, &pkey) <= 0)
		report_key_generate_error(ctx, "unable to generate key");

	EVP_PKEY_CTX_free(ctx);
	return pkey;
}

static void
pg_attribute_noreturn() report_key_import_error(BIO *mem)
{
	BIO_free(mem);
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("key import error"),
			 errdetail("Error: %s", ERR_reason_error_string(ERR_get_error()))));
}

/*
 * Import a key from TEXT in PKCS#8 format.
 */
static EVP_PKEY *
key_import(text *key_buf)
{
	const char *ptr = VARDATA_ANY(key_buf);
	size_t key_len = VARSIZE_ANY_EXHDR(key_buf);
	BIO *mem = BIO_new(BIO_s_mem());
	EVP_PKEY *pkey = NULL;

	if (BIO_write(mem, ptr, key_len) != key_len)
		report_key_import_error(mem);

	if (!PEM_read_bio_PrivateKey(mem, &pkey, NULL, NULL))
		report_key_import_error(mem);

	BIO_free(mem);
	return pkey;
}

/*
 * Export a key to TEXT in PKCS#8 format.
 */
static text *
key_export(EVP_PKEY *pkey)
{
	BIO *mem = BIO_new(BIO_s_mem());
	text *ptr = NULL;

	if (PEM_write_bio_PKCS8PrivateKey(mem, pkey, NULL, NULL, 0, NULL, NULL))
	{
		char *data = NULL;
		long length = BIO_get_mem_data(mem, &data);
		ptr = palloc(VARHDRSZ + length);
		memcpy(VARDATA(ptr), data, length);
		SET_VARSIZE(ptr, VARHDRSZ + length);
		BIO_set_flags(mem, BIO_FLAGS_MEM_RDONLY);
	}
	BIO_free(mem);
	return ptr;
}

TS_FUNCTION_INFO_V1(ts_pg_generate_key);
TS_FUNCTION_INFO_V1(ts_pg_check_key);

Datum
ts_pg_generate_key(PG_FUNCTION_ARGS)
{
	EVP_PKEY *pkey = ts_cert_generate_key();
	text *res = key_export(pkey);
	EVP_PKEY_free(pkey);
	PG_RETURN_TEXT_P(res);
}

/*
 * Verify an SSL key.
 *
 * For keys generated by `ts_pg_generate_key` you can verify the key and make
 * sure that it is correct. The key is assumed to be in PKCS#8 format.
 */

Datum
ts_pg_check_key(PG_FUNCTION_ARGS)
{
	text *const key_buf = PG_GETARG_TEXT_PP(0);
	EVP_PKEY *const pkey = key_import(key_buf);
	RSA *rsa = EVP_PKEY_get1_RSA(pkey);

	/* Need to use RSA_check_key here since Alpine is using OpenSSL 1.0.2 */
	int r = RSA_check_key(rsa);

	EVP_PKEY_free(pkey);

	if (r != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("RSA key error"),
				 errdetail("Error: %s", ERR_reason_error_string(ERR_get_error()))));

	PG_RETURN_BOOL(r == 1);
}
