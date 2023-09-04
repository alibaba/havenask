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
#include "autil/cipher/AESCipherDecrypter.h"
#include "autil/cipher/ICipherStream.h"
#include "openssl/evp.h"

namespace autil {
namespace cipher {

class MemoryDataPipeline;

class AESCipherStreamDecrypter : public AESCipherDecrypter, public ICipherStream {
public:
    AESCipherStreamDecrypter(std::unique_ptr<AESCipherUtility> utility, size_t capacity, size_t readTimeoutInMs);
    ~AESCipherStreamDecrypter();

public:
    bool append(const unsigned char *data, size_t dataLen) override;
    size_t get(unsigned char *buffer, size_t bufferLen) override;

    bool seal() override;
    bool isSealed() const { return _status == ST_SEALED; }
    size_t getMemoryUse() const;
    bool isEof() const override;

private:
    enum InnerStatus {
        ST_UNKNOWN = 0,
        ST_INIT = 1,
        ST_SEALED = 2,
        ST_ERROR = 3,
    };

    EVP_CIPHER_CTX *_ctx = nullptr;
    MemoryDataPipeline *_pipe = nullptr;
    std::string _saltHeader;
    size_t _capacity;
    size_t _readTimeoutInMs;
    volatile InnerStatus _status = ST_UNKNOWN;
    static const size_t DEFAULT_PIPE_FREE_BLOCK_COUNT;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cipher
} // namespace autil
