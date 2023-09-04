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

#include "autil/cipher/AESCipherCommon.h"
#include "build_service/common_define.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class CipherFormatFileDocumentReader : public FormatFileDocumentReader
{
public:
    CipherFormatFileDocumentReader(autil::cipher::CipherOption option, const std::string& docPrefix,
                                   const std::string& docSuffix);
    ~CipherFormatFileDocumentReader();

    CipherFormatFileDocumentReader(const CipherFormatFileDocumentReader&) = delete;
    CipherFormatFileDocumentReader& operator=(const CipherFormatFileDocumentReader&) = delete;
    CipherFormatFileDocumentReader(CipherFormatFileDocumentReader&&) = delete;
    CipherFormatFileDocumentReader& operator=(CipherFormatFileDocumentReader&&) = delete;

protected:
    FileReaderBase* createInnerFileReader(const std::string& fileName) override;

private:
    autil::cipher::CipherOption _option;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CipherFormatFileDocumentReader);

}} // namespace build_service::reader
