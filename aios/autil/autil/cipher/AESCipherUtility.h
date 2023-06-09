/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "autil/Log.h"
#include "autil/cipher/AESCipherCommon.h"
#include "openssl/evp.h"

namespace autil { namespace cipher {

class AESCipherUtility
{
public:
    static constexpr char magic[] = "Salted__";

public:
    AESCipherUtility(CipherType type);
    ~AESCipherUtility();

public:
    /* prepare param for cal key & iv with password */
    AESCipherUtility& setDigistType(DigistType type);
    AESCipherUtility& enablePbkdf2(int iter = 10000 /* PBKDF2_ITER_DEFAULT = 10000 */);
    AESCipherUtility& enableSalt(bool flag);
    AESCipherUtility& setPassword(const std::string& passwd);
    
    bool setSalt(const std::string& hsalt); //hex string
    bool setSaltHeader(const std::string& headerStr); // magic & salt binary
    /* when use user defined key & iv, salt is useless */
    bool setKeyAndIv(const std::string& hKey, const std::string& hiv);   //hex string

    bool needCalculate() const { return _keyLen <= 0 || _ivLen <= 0; }
    bool calculate();

public:
    const unsigned char* getKey(size_t& len) const;
    const unsigned char* getIv(size_t& len) const;
    const unsigned char* getSalt(size_t& len) const;

    std::string getKeyHexString() const;
    std::string getIvHexString() const;
    std::string getSaltHexString() const;
    
    bool needSaltHeader() const { return _enableSalt && !_setSalt; /* use random salt*/ }
    std::string getSaltHeader() const;
    const EVP_CIPHER* getCipher() const { return _cipher; }
    std::string debugString() const;
    size_t getCipherBlockSize() const;
    size_t getMaxEncryptDataLength(size_t len) const;
    size_t getSaltHeaderLen() const;
    
private:
    int hexChar2Int(char c) const {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }

    bool hex2Bytes(const std::string& hexStr, unsigned char* bytes) const;
    std::string byte2HexStr(const unsigned char* byte, size_t len) const;
    
private:
    static constexpr int SALT_LEN = 8;
    
    unsigned char _key[EVP_MAX_KEY_LENGTH];
    unsigned char _iv[EVP_MAX_IV_LENGTH];
    unsigned char _salt[SALT_LEN];
    int _keyLen = 0;
    int _ivLen = 0;
    std::string _passwd;
    bool _setSalt = false;
    
    const EVP_CIPHER *_cipher = nullptr;
    const EVP_MD *_dgst = nullptr;
    bool _enableSalt = false;    
    bool _usePbkdf2 = false;
    int _pbkdf2Iter = 0;
    
private:
    AUTIL_LOG_DECLARE();
};

}}

