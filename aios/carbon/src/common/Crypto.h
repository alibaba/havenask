/**
 * @file re_hmac.h  Interface to HMAC functions
 *
 * Copyright (C) 2010 Creytiv.com
 */

#ifndef CARBON_CRYPTO_H
#define CARBON_CRYPTO_H

#include "carbon/Log.h"
#include "common/common.h"
BEGIN_CARBON_NAMESPACE(common);

class Crypto
{
public:
    Crypto();
    ~Crypto();
private:
    Crypto(const Crypto &);
    Crypto& operator=(const Crypto &);
public:
   /**
    * Function to compute the digest
    *
    * @param k   Secret key
    * @param lk  Length of the key in bytes
    * @param d   Data
    * @param ld  Length of data in bytes
    * @param out Digest output
    * @param t   Size of digest output
    */    
    static void hmacsha1(const uint8_t *k,   /* secret key */
                         size_t lk,  /* length of the key in bytes */
                         const uint8_t *d,   /* data */
                         size_t ld,  /* length of data in bytes */
                         uint8_t *out, /* output buffer, at least "t" bytes */
                         size_t *t);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(Crypto);

END_CARBON_NAMESPACE(common);

#endif //CARBON_CRYPTO_H
