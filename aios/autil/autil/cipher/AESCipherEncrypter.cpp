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
#include "autil/cipher/AESCipherEncrypter.h"
#include "autil/cipher/AESCipherUtility.h"
#include "openssl/err.h"
#include <string.h>
#include <unistd.h>

namespace autil { namespace cipher {
AUTIL_LOG_SETUP(autil, AESCipherEncrypter);

AESCipherEncrypter::AESCipherEncrypter(std::unique_ptr<AESCipherUtility> utility)
    : _utility(std::move(utility))
{
    assert(_utility);
}

AESCipherEncrypter::~AESCipherEncrypter() 
{
}

bool AESCipherEncrypter::encryptMessage(const std::string& plainText, std::string& cipherText)
{
    if (!_utility->calculate()) {
        AUTIL_LOG(ERROR, "utility calculate fail.");
        return false;
    }

    cipherText.resize(_utility->getMaxEncryptDataLength(plainText.size()));
    EVP_CIPHER_CTX ctx;
    EVP_CIPHER_CTX_init(&ctx);
    size_t len;
    if (EVP_EncryptInit_ex(&ctx, _utility->getCipher(), nullptr,
                           _utility->getKey(len), _utility->getIv(len)) <= 0)
    {
        AUTIL_LOG(ERROR, "Failed to initialize encryption operation, error:[%s].",
                  toErrorString(ERR_get_error()).c_str());
        EVP_CIPHER_CTX_cleanup(&ctx);        
        return false;
    }

    size_t cipherLen = 0;
    unsigned char *cipherBuffer = (unsigned char*)cipherText.data();
    if (_utility->needSaltHeader()) {
        std::string headerStr = _utility->getSaltHeader();
        memcpy(cipherBuffer, headerStr.data(), headerStr.size());
        cipherBuffer += headerStr.size();
        cipherLen += headerStr.size();        
    }

    int encryptLen = 0;
    if (EVP_EncryptUpdate(&ctx, cipherBuffer, &encryptLen,
                          (const unsigned char*)plainText.c_str(), plainText.size()) <= 0)
    {
        AUTIL_LOG(ERROR, "Encryption failed, error:[%s].", toErrorString(ERR_get_error()).c_str());
        EVP_CIPHER_CTX_cleanup(&ctx);
        return false;
    }
    cipherBuffer += encryptLen;
    cipherLen += encryptLen;

    int finalLen = 0;
    if (EVP_EncryptFinal_ex(&ctx, cipherBuffer, &finalLen) <= 0) {
        AUTIL_LOG(ERROR, "Final encryption failed, error:[%s].",
                  toErrorString(ERR_get_error()).c_str());
        EVP_CIPHER_CTX_cleanup(&ctx);        
        return false;
    }
    cipherLen += finalLen;
    cipherText.resize(cipherLen);
    EVP_CIPHER_CTX_cleanup(&ctx);
    return true;
}


std::string AESCipherEncrypter::toErrorString(unsigned long err)
{
    char errBuf[256];
    ERR_error_string(err, errBuf);
    return std::string(errBuf);
}

std::string AESCipherEncrypter::getKeyHexString() const
{
    return _utility->getKeyHexString();
}

std::string AESCipherEncrypter::getIvHexString() const
{
    return _utility->getIvHexString();
}

std::string AESCipherEncrypter::getSaltHexString() const
{
    return _utility->getSaltHexString();
}

}}

