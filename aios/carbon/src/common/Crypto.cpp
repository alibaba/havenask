/**
 * @file hmac_sha1.c  Implements HMAC-SHA1 as of RFC 2202
 *
 * Copyright (C) 2010 Creytiv.com
 */
#include "common/Crypto.h"

#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/err.h>

using namespace std;
BEGIN_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(common, Crypto);

Crypto::Crypto() {
}

Crypto::~Crypto() {
}

void Crypto::hmacsha1(const uint8_t *k,   /* secret key */
                      size_t lk,  /* length of the key in bytes */
                      const uint8_t *d,   /* data */
                      size_t ld,  /* length of data in bytes */
                      uint8_t *out, /* output buffer, at least "t" bytes */
                      size_t *t)
{
    if (!HMAC(EVP_sha1(), k, (int)lk, d, ld, out, (unsigned int*)t)) {
        ERR_clear_error();
    }
}

END_CARBON_NAMESPACE(common);

