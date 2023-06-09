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
#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"

using namespace std;
using namespace build_service::document;
using namespace swift::common;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, SwiftFieldFilterRawDocumentParser);

SwiftFieldFilterRawDocumentParser::SwiftFieldFilterRawDocumentParser() {}

SwiftFieldFilterRawDocumentParser::~SwiftFieldFilterRawDocumentParser() {}

bool SwiftFieldFilterRawDocumentParser::parse(const string& docString, RawDocument& rawDoc)
{
    return parse((const char*)docString.data(), docString.size(), rawDoc);
}

bool SwiftFieldFilterRawDocumentParser::parse(const char* dataStr, size_t dataLen, document::RawDocument& rawDoc)
{
    if (!_fieldGroupReader.fromProductionString(dataStr, dataLen)) {
        string errorMsg = "failed to parse from swift field filter message, data[" + string(dataStr, dataLen) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    size_t fieldCount = _fieldGroupReader.getFieldSize();
    for (size_t i = 0; i < fieldCount; ++i) {
        const Field* field = _fieldGroupReader.getField(i);
        assert(field);
        if (field->isExisted && !field->name.empty()) {
            rawDoc.setField(field->name.data(), field->name.size(), field->value.data(), field->value.size());
        }
    }

    if (rawDoc.getFieldCount() == 0) {
        BS_LOG(WARN, "fieldCount is zero");
        return false;
    }
    if (!rawDoc.exist(CMD_TAG)) {
        rawDoc.setField(CMD_TAG, CMD_ADD);
    }
    return true;
}

}} // namespace build_service::reader
