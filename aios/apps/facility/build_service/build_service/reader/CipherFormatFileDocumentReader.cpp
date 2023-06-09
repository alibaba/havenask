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
#include "build_service/reader/CipherFormatFileDocumentReader.h"
#include "build_service/reader/AESCipherFileReader.h"
#include "build_service/reader/AESCipherGZipFileReader.h"

using namespace std;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, CipherFormatFileDocumentReader);

CipherFormatFileDocumentReader::CipherFormatFileDocumentReader(autil::cipher::CipherOption option,
                                                               const std::string& docPrefix,
                                                               const std::string& docSuffix)
    : FormatFileDocumentReader(docPrefix, docSuffix)
    , _option(option)
{
}

CipherFormatFileDocumentReader::~CipherFormatFileDocumentReader() {}

FileReaderBase* CipherFormatFileDocumentReader::createInnerFileReader(const string& fileName)
{
    FileReaderBase::FileType fileType = FileReaderBase::getFileType(fileName);
    if (fileType == FileReaderBase::GZIP_TYPE) {
        return new AESCipherGZipFileReader(_option, _bufferSize);
    }
    return new AESCipherFileReader(_option);
}

}} // namespace build_service::reader
