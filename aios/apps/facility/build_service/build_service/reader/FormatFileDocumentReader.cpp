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
#include "build_service/reader/FormatFileDocumentReader.h"

using namespace std;
namespace build_service { namespace reader {
BS_LOG_SETUP(reader, FormatFileDocumentReader);

FormatFileDocumentReader::FormatFileDocumentReader(const string& docPrefix, const string& docSuffix,
                                                   uint32_t bufferSize)
    : FileDocumentReader(bufferSize)
    , _docPrefix(docPrefix)
    , _docSuffix(docSuffix)
{
    uint32_t maxPrefixLen = std::max(_docPrefix.size(), _docSuffix.size());
    _bufferSize = _bufferSize < maxPrefixLen ? maxPrefixLen : _bufferSize;
}

FormatFileDocumentReader::~FormatFileDocumentReader() {}

bool FormatFileDocumentReader::doRead(std::string& docStr)
{
    if (!findSeparator(_docPrefix, docStr)) {
        return false;
    }
    docStr.clear();
    docStr.reserve(32 * 1024);
    return findSeparator(_docSuffix, docStr);
}

bool FormatFileDocumentReader::findSeparator(const Separator& separator, string& docStr)
{
    uint32_t sepLen = separator.size();
    if (sepLen == 0) {
        return true;
    }

    while (true) {
        char* pos = (char*)separator.findInBuffer(_bufferCursor, _bufferEnd);
        if (pos != NULL) {
            readUntilPos(pos, sepLen, docStr);
            return true;
        }
        char* readEnd = _bufferEnd - sepLen;
        if (readEnd > _bufferCursor) {
            readUntilPos(readEnd, 0, docStr);
        }
        if (!fillBuffer()) {
            return false;
        }
    }
}

}} // namespace build_service::reader
