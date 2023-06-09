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
#include "build_service/reader/FixLenBinaryFileDocumentReader.h"

using namespace std;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, FixLenBinaryFileDocumentReader);

FixLenBinaryFileDocumentReader::FixLenBinaryFileDocumentReader(size_t fixLen, uint32_t bufferSize)
    : FileDocumentReader(bufferSize)
    , _fixLen(fixLen)
{
}

FixLenBinaryFileDocumentReader::~FixLenBinaryFileDocumentReader() {}

bool FixLenBinaryFileDocumentReader::doRead(string& docStr)
{
    size_t targetFileOffset = (_fileOffset + _fixLen - 1) / _fixLen * _fixLen;
    size_t alignSize = targetFileOffset - _fileOffset;
    if (alignSize > 0) {
        string tmp;
        tmp.resize(alignSize);
        read((char*)tmp.data(), alignSize);
    }

    docStr.clear();
    docStr.resize(_fixLen);
    char* buffer = (char*)docStr.data();
    return read(buffer, _fixLen);
}

}} // namespace build_service::reader
