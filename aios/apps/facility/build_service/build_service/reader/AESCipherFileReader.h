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

#include <thread>

#include "autil/cipher/AESCipherCommon.h"
#include "autil/cipher/AESCipherStreamDecrypter.h"
#include "build_service/common_define.h"
#include "build_service/reader/FileReaderBase.h"
#include "build_service/util/Log.h"
#include "fslib/fslib.h"

namespace build_service { namespace reader {

class AESCipherFileReader : public FileReaderBase
{
public:
    AESCipherFileReader(autil::cipher::CipherOption option);
    ~AESCipherFileReader();

private:
    AESCipherFileReader(const AESCipherFileReader&) = delete;
    AESCipherFileReader& operator=(const AESCipherFileReader&) = delete;
    AESCipherFileReader(AESCipherFileReader&&) = delete;
    AESCipherFileReader& operator=(AESCipherFileReader&&) = delete;

public:
    bool init(const std::string& fileName, int64_t offset) override;
    bool get(char* output, uint32_t size, uint32_t& sizeUsed) override;
    bool good() override;
    bool isEof() override;
    int64_t getReadBytes() const override;
    bool seek(int64_t offset) override;

protected:
    virtual FileReaderBase* createInnerFileReader();
    bool skipToOffset(int64_t offset);
    bool innerGet(char* output, uint32_t size, uint32_t& sizeUsed);

protected:
    std::unique_ptr<autil::cipher::AESCipherStreamDecrypter> _decrypter;
    std::unique_ptr<std::thread> _readThread;
    FileReaderBase* _innerFile = nullptr;
    volatile bool _hasError;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AESCipherFileReader);

}} // namespace build_service::reader
