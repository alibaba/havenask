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
#include "autil/cipher/AESCipherStreamDecrypter.h"

#include "autil/EnvUtil.h"
#include "autil/cipher/AESCipherUtility.h"
#include "autil/cipher/MemoryDataPipeline.h"
#include "openssl/err.h"

namespace autil {
namespace cipher {
AUTIL_LOG_SETUP(autil, AESCipherStreamDecrypter);

const size_t AESCipherStreamDecrypter::DEFAULT_PIPE_FREE_BLOCK_COUNT =
    EnvUtil::getEnv("max_cipher_pipe_free_block_count", 10);

AESCipherStreamDecrypter::AESCipherStreamDecrypter(std::unique_ptr<AESCipherUtility> utility,
                                                   size_t capacity,
                                                   size_t readTimeoutInMs)
    : AESCipherDecrypter(std::move(utility)), _capacity(capacity), _readTimeoutInMs(readTimeoutInMs) {
    _ctx = EVP_CIPHER_CTX_new();
}

AESCipherStreamDecrypter::~AESCipherStreamDecrypter() {
    if (_ctx != nullptr) {
        EVP_CIPHER_CTX_free(_ctx);
    }
    if (_pipe != nullptr) {
        delete _pipe;
    }
}

bool AESCipherStreamDecrypter::append(const unsigned char *data, size_t dataLen) {
    if (_status == ST_ERROR || _status == ST_SEALED) {
        return false;
    }

    if (_status == ST_UNKNOWN) {
        _status = ST_INIT;
        if (_useRandomSalt) {
            size_t saltHeaderLen = _utility->getSaltHeaderLen();
            if ((_saltHeader.size() + dataLen) < saltHeaderLen) {
                _saltHeader += std::string((const char *)data, dataLen);
                _status = ST_UNKNOWN;
                return true;
            }
            size_t saltSkipLen = saltHeaderLen - _saltHeader.size();
            _saltHeader += std::string((const char *)data, saltSkipLen);
            if (_utility->needCalculate()) {
                if (_saltHeader.find(AESCipherUtility::magic) != 0) {
                    AUTIL_LOG(ERROR, "cipherText not starts with salt magic.");
                    _status = ST_ERROR;
                    return false;
                }
                _utility->setSaltHeader(_saltHeader);
                if (!_utility->calculate()) {
                    AUTIL_LOG(ERROR, "utility calculate fail.");
                    _status = ST_ERROR;
                    return false;
                }
            } else {
                auto expectHeader = _utility->getSaltHeader();
                if (_saltHeader != expectHeader) {
                    // maybe call decryptMsg more than once, and different cipherText with different salt header
                    AUTIL_LOG(ERROR, "salt header miss match between cipherText & utility.");
                    _status = ST_ERROR;
                    return false;
                }
            }
            data += saltSkipLen;
            dataLen -= saltSkipLen;
        } else if (!_utility->calculate()) {
            AUTIL_LOG(ERROR, "utility calculate fail.");
            _status = ST_ERROR;
            return false;
        }
        size_t len;
        if (EVP_DecryptInit_ex(_ctx, _utility->getCipher(), nullptr, _utility->getKey(len), _utility->getIv(len)) <=
            0) {
            AUTIL_LOG(ERROR,
                      "Failed to initialize decryption operation, error:[%s].",
                      toErrorString(ERR_get_error()).c_str());
            _status = ST_ERROR;
            return false;
        }
        _pipe = new MemoryDataPipeline(
            _utility->getCipherBlockSize() * 4096, _capacity, _readTimeoutInMs, DEFAULT_PIPE_FREE_BLOCK_COUNT);
    }

    const size_t batchSize = _utility->getCipherBlockSize() * 64;
    const size_t bufSize = batchSize + _utility->getCipherBlockSize(); /* make efficient buffer room */
    size_t totalDecryptLen = 0;
    while (totalDecryptLen < dataLen) {
        size_t tmpLen = std::min(batchSize, dataLen - totalDecryptLen);
        unsigned char *buf = (unsigned char *)_pipe->getMemoryBuffer(bufSize);
        assert(buf != nullptr);
        int decryptLen = 0;
        if (EVP_DecryptUpdate(_ctx, buf, &decryptLen, (const unsigned char *)(data + totalDecryptLen), tmpLen) <= 0) {
            AUTIL_LOG(ERROR, "Decryption failed, error:[%s].", toErrorString(ERR_get_error()).c_str());
            _status = ST_ERROR;
            return false;
        }
        _pipe->increaseDataLength(decryptLen);
        totalDecryptLen += tmpLen;
    }
    return true;
}

bool AESCipherStreamDecrypter::seal() {
    if (_status == ST_ERROR) {
        return false;
    }
    if (_status == ST_SEALED || _status == ST_UNKNOWN) {
        _status = ST_SEALED;
        return true;
    }
    const size_t bufSize = _utility->getCipherBlockSize() * 8;
    unsigned char *buf = (unsigned char *)_pipe->getMemoryBuffer(bufSize);
    assert(buf != nullptr);
    int finalLen = 0;
    if (EVP_DecryptFinal_ex(_ctx, buf, &finalLen) <= 0) {
        AUTIL_LOG(ERROR, "Final decryption failed, error:[%s].", toErrorString(ERR_get_error()).c_str());
        _status = ST_ERROR;
        return false;
    }
    _pipe->increaseDataLength(finalLen);
    __asm__ __volatile__("" ::: "memory");
    _status = ST_SEALED;
    _pipe->seal();
    return true;
}

size_t AESCipherStreamDecrypter::get(unsigned char *buffer, size_t bufferLen) {
    return (_pipe != nullptr) ? _pipe->read((char *)buffer, bufferLen) : 0;
}

size_t AESCipherStreamDecrypter::getMemoryUse() const { return (_pipe != nullptr) ? _pipe->getCurrentMemoryUse() : 0; }

bool AESCipherStreamDecrypter::isEof() const {
    if (!isSealed()) {
        return false;
    }
    return !_pipe || _pipe->isEof();
}

} // namespace cipher
} // namespace autil
