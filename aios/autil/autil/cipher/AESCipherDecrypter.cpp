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
#include "autil/cipher/AESCipherDecrypter.h"

#include <unistd.h>

#include "autil/cipher/AESCipherUtility.h"
#include "openssl/err.h"

namespace autil {
namespace cipher {
AUTIL_LOG_SETUP(autil, AESCipherDecrypter);

AESCipherDecrypter::AESCipherDecrypter(std::unique_ptr<AESCipherUtility> utility) : _utility(std::move(utility)) {
    assert(_utility);
    _useRandomSalt = _utility->needSaltHeader();
}

AESCipherDecrypter::~AESCipherDecrypter() {}

bool AESCipherDecrypter::decryptMessage(const std::string &cipherText, std::string &plainText) {
    unsigned char *cipherData = (unsigned char *)cipherText.data();
    int cipherDataLen = (int)cipherText.size();
    if (_useRandomSalt) {
        if (cipherDataLen < _utility->getSaltHeaderLen()) {
            AUTIL_LOG(ERROR, "invalid cipherText length [%d]", cipherDataLen);
            return false;
        }
        auto saltHeader = cipherText.substr(0, _utility->getSaltHeaderLen());
        if (_utility->needCalculate()) {
            if (saltHeader.find(AESCipherUtility::magic) != 0) {
                AUTIL_LOG(ERROR, "cipherText not starts with salt magic.");
                return false;
            }
            if (!_utility->setSaltHeader(saltHeader)) {
                return false;
            }
            if (!_utility->calculate()) {
                AUTIL_LOG(ERROR, "utility calculate fail.");
                return false;
            }
        } else {
            auto expectHeader = _utility->getSaltHeader();
            if (saltHeader != expectHeader) {
                // maybe call decryptMsg more than once, and different cipherText with different salt header
                AUTIL_LOG(ERROR, "salt header miss match between cipherText & utility.");
                return false;
            }
        }
        cipherData += _utility->getSaltHeaderLen();
        cipherDataLen -= _utility->getSaltHeaderLen();
    } else if (!_utility->calculate()) {
        AUTIL_LOG(ERROR, "utility calculate fail.");
        return false;
    }

    plainText.resize(cipherText.size() + _utility->getCipherBlockSize());
    unsigned char *plainBuffer = (unsigned char *)plainText.data();
    EVP_CIPHER_CTX ctx;
    EVP_CIPHER_CTX_init(&ctx);
    size_t len;
    if (EVP_DecryptInit_ex(&ctx, _utility->getCipher(), nullptr, _utility->getKey(len), _utility->getIv(len)) <= 0) {
        AUTIL_LOG(
            ERROR, "Failed to initialize decryption operation, error:[%s].", toErrorString(ERR_get_error()).c_str());
        EVP_CIPHER_CTX_cleanup(&ctx);
        return false;
    }

    int decryptLen = 0;
    if (EVP_DecryptUpdate(&ctx, plainBuffer, &decryptLen, cipherData, cipherDataLen) <= 0) {
        AUTIL_LOG(ERROR, "Decryption failed, error:[%s].", toErrorString(ERR_get_error()).c_str());
        EVP_CIPHER_CTX_cleanup(&ctx);
        return false;
    }
    plainBuffer += decryptLen;

    int finalLen = 0;
    if (EVP_DecryptFinal_ex(&ctx, plainBuffer, &finalLen) <= 0) {
        AUTIL_LOG(ERROR, "Final decryption failed, error:[%s].", toErrorString(ERR_get_error()).c_str());
        EVP_CIPHER_CTX_cleanup(&ctx);
        return false;
    }
    decryptLen += finalLen;
    plainText.resize(decryptLen);
    EVP_CIPHER_CTX_cleanup(&ctx);
    return true;
}

std::string AESCipherDecrypter::toErrorString(unsigned long err) {
    char errBuf[256];
    ERR_error_string(err, errBuf);
    return std::string(errBuf);
}

std::string AESCipherDecrypter::getKeyHexString() const { return _utility->getKeyHexString(); }

std::string AESCipherDecrypter::getIvHexString() const { return _utility->getIvHexString(); }

std::string AESCipherDecrypter::getSaltHexString() const { return _utility->getSaltHexString(); }

} // namespace cipher
} // namespace autil
