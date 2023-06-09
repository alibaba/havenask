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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/GZipFileReader.h"
#include "autil/cipher/AESCipherCommon.h"

namespace build_service {
namespace reader {

/* first: make a gzip file, second: encrypt the gzip file with openssl AES cipher */
/* CipherAfterGZip is preferred, make gzip by plain data may have good compress ratio */
class AESCipherGZipFileReader : public GZipFileReader
{
public:
    AESCipherGZipFileReader(autil::cipher::CipherOption option, uint32_t bufferSize);
    ~AESCipherGZipFileReader();

    AESCipherGZipFileReader(const AESCipherGZipFileReader &) = delete;
    AESCipherGZipFileReader& operator=(const AESCipherGZipFileReader &) = delete;
    AESCipherGZipFileReader(AESCipherGZipFileReader&&) = delete;
    AESCipherGZipFileReader& operator=(AESCipherGZipFileReader&&) = delete;
    
protected:
    FileReaderBase* createInnerFileReader() override;

private:
    autil::cipher::CipherOption _option;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AESCipherGZipFileReader);

}
}

