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
#include "build_service/reader/StandardRawDocumentParser.h"

using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, StandardRawDocumentParser);

StandardRawDocumentParser::StandardRawDocumentParser(const string& fieldSep, const string& keyValueSep)
    : _fieldSep(fieldSep)
    , _keyValueSep(keyValueSep)
{
}

StandardRawDocumentParser::~StandardRawDocumentParser() {}

bool StandardRawDocumentParser::parse(const string& docString, RawDocument& rawDoc)
{
    const char* docCursor = docString.data();
    const char* docEnd = docString.data() + docString.size();

    while (docCursor < docEnd) {
        pair<const char*, size_t> fieldName = findNext(_keyValueSep, docCursor, docEnd);
        if (fieldName.first == NULL || docCursor > docEnd) {
            break;
        }

        pair<const char*, size_t> fieldValue;
        if (docCursor == docEnd) {
            // set last empty value
            fieldValue = make_pair(docEnd, 0);
        } else {
            fieldValue = findNext(_fieldSep, docCursor, docEnd);
        }

        if (fieldValue.first == NULL) {
            break;
        }
        // continue after read field value.
        trim(fieldName.first, fieldName.second);
        if (fieldName.second == 0) {
            string errorMsg = "fieldName should not be empty after trim";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            continue;
        }
        rawDoc.setField(fieldName.first, fieldName.second, fieldValue.first, fieldValue.second);
    }
    if (rawDoc.getFieldCount() == 0) {
        BS_LOG(WARN,
               "fieldCount is zero, docString is: %s"
               ", field_separator: %s, kv_separator: %s",
               docString.c_str(), _fieldSep.getSeperator().c_str(), _keyValueSep.getSeperator().c_str());
        return false;
    }
    if (!rawDoc.exist(CMD_TAG)) {
        rawDoc.setField(CMD_TAG, CMD_ADD);
    }
    return true;
}

pair<const char*, size_t> StandardRawDocumentParser::findNext(const Separator& sep, const char*& docCursor,
                                                              const char* docEnd)
{
    const char* strBegin = docCursor;
    const char* strEnd = sep.findInBuffer(strBegin, docEnd);
    if (!strEnd) {
        strEnd = docEnd;
    }
    size_t strLen = strEnd - strBegin;
    docCursor = strEnd + sep.size();
    return make_pair(strBegin, strLen);
}

void StandardRawDocumentParser::trim(const char*& ptr, size_t& len)
{
    const char* p = ptr;
    const char* q = ptr + len;

    while (p < q && (*p == ' ' || *p == '\n')) {
        p++;
    }

    while (q > p && (*(q - 1) == ' ' || *(q - 1) == '\n')) {
        q--;
    }

    if (likely(q == p + len)) {
        return;
    } else {
        ptr = p;
        len = q - p;
    }
}

}} // namespace build_service::reader
