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
#include "autil/cipher/AESCipherStreamEncrypter.h"
#include "autil/cipher/AESCipherUtility.h"
#include "autil/cipher/MemoryDataPipeline.h"
#include "autil/EnvUtil.h"
#include "openssl/err.h"

namespace autil { namespace cipher {
AUTIL_LOG_SETUP(autil, AESCipherStreamEncrypter);

const size_t AESCipherStreamEncrypter::DEFAULT_PIPE_FREE_BLOCK_COUNT =
    autil::EnvUtil::getEnv("max_cipher_pipe_free_block_count", 10);
        
AESCipherStreamEncrypter::AESCipherStreamEncrypter(
        std::unique_ptr<AESCipherUtility> utility, size_t capacity, size_t readTimeoutInMs)
    : AESCipherEncrypter(std::move(utility))
    , _capacity(capacity)
    , _readTimeoutInMs(readTimeoutInMs)
{
    _ctx = EVP_CIPHER_CTX_new();
}

AESCipherStreamEncrypter::~AESCipherStreamEncrypter() 
{
    if (_ctx != nullptr) {
        EVP_CIPHER_CTX_free(_ctx);
    }
    if (_pipe != nullptr) {
        delete _pipe;
    }
}

bool AESCipherStreamEncrypter::append(const unsigned char* data, size_t dataLen)
{
    if (_status == ST_ERROR || _status == ST_SEALED) {
        return false;
    }
    
    if (_status == ST_UNKNOWN) {
        _status = ST_INIT;
        if (!_utility->calculate()) {
            AUTIL_LOG(ERROR, "utility calculate fail.");
            _status = ST_ERROR;            
            return false;
        }
        size_t len;
        if (EVP_EncryptInit_ex(_ctx, _utility->getCipher(), nullptr,
                               _utility->getKey(len), _utility->getIv(len)) <= 0)
        {
            AUTIL_LOG(ERROR, "Failed to initialize encryption operation, error:[%s].",
                      toErrorString(ERR_get_error()).c_str());
            _status = ST_ERROR;
            return false;
        }
        _pipe = new MemoryDataPipeline(_utility->getCipherBlockSize() * 4096,
                _capacity, _readTimeoutInMs, DEFAULT_PIPE_FREE_BLOCK_COUNT);
        if (_utility->needSaltHeader()) {
            std::string headerStr = _utility->getSaltHeader();
            _pipe->write(headerStr.data(), headerStr.size());
        }
    }

    const size_t batchSize = _utility->getCipherBlockSize() * 64;
    const size_t bufSize = batchSize + _utility->getCipherBlockSize(); /* make efficient room */
    size_t totalEncryptLen = 0;
    while (totalEncryptLen < dataLen) {
        size_t tmpLen = std::min(batchSize, dataLen - totalEncryptLen);
        unsigned char* buf = (unsigned char*)_pipe->getMemoryBuffer(bufSize);
        assert(buf != nullptr);
        int encryptLen = 0;
        if (EVP_EncryptUpdate(_ctx, buf, &encryptLen,
                              (const unsigned char*)(data + totalEncryptLen), tmpLen) <= 0)
        {
            AUTIL_LOG(ERROR, "Encryption failed, error:[%s].", toErrorString(ERR_get_error()).c_str());
            _status = ST_ERROR;
            return false;
        }
        _pipe->increaseDataLength(encryptLen);
        totalEncryptLen += tmpLen;
    }
    return true;
}

bool AESCipherStreamEncrypter::seal()
{
    if (_status == ST_ERROR) {
        return false;
    }
    if (_status == ST_SEALED || _status == ST_UNKNOWN) {
        _status = ST_SEALED;
        return true;
    }
    const size_t bufSize = _utility->getCipherBlockSize() * 8;
    unsigned char* buf = (unsigned char*)_pipe->getMemoryBuffer(bufSize);
    assert(buf != nullptr);
    int finalLen = 0;
    if (EVP_EncryptFinal_ex(_ctx, buf, &finalLen) <= 0) {
        AUTIL_LOG(ERROR, "Final encryption failed, error:[%s].",
                  toErrorString(ERR_get_error()).c_str());
        _status = ST_ERROR;
        return false;
    }
    _pipe->increaseDataLength(finalLen);
    __asm__ __volatile__("" ::: "memory");
    _status = ST_SEALED;
    _pipe->seal();    
    return true;
}

size_t AESCipherStreamEncrypter::get(unsigned char* buffer, size_t bufferLen)
{
    return (_pipe != nullptr) ? _pipe->read((char*)buffer, bufferLen) : 0;
}

size_t AESCipherStreamEncrypter::getMemoryUse() const
{
    return (_pipe != nullptr) ? _pipe->getCurrentMemoryUse() : 0;
}

bool AESCipherStreamEncrypter::isEof() const
{
    if (!isSealed()) {
        return false;
    }
    return !_pipe || _pipe->isEof();    
}


}}

