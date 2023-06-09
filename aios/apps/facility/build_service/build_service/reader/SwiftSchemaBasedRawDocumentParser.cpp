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
#include "build_service/reader/SwiftSchemaBasedRawDocumentParser.h"

#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"
#include "swift/common/SchemaReader.h"

using namespace swift::common;
using namespace swift::client;
using namespace swift::protocol;
using namespace std;
using namespace autil;
using namespace build_service::document;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, SwiftSchemaBasedRawDocumentParser);

SwiftSchemaBasedRawDocumentParser::SwiftSchemaBasedRawDocumentParser() : _swiftReader(NULL), _fieldFilterParser(NULL)
{
    _fieldFilterParser = new SwiftFieldFilterRawDocumentParser();
}

SwiftSchemaBasedRawDocumentParser::~SwiftSchemaBasedRawDocumentParser() { DELETE_AND_SET_NULL(_fieldFilterParser); }

bool SwiftSchemaBasedRawDocumentParser::parse(const std::string& docString, document::RawDocument& rawDoc)
{
    if (!_swiftReader) {
        BS_LOG(ERROR, "swiftReader is NULL!");
        return false;
    }

    DataType type = parseDataType(docString);
    StringView data = parseData(docString);
    if (type == DATA_TYPE_SCHEMA) {
        return parseSchemaDocument(data, rawDoc);
    }

    if (type == DATA_TYPE_FIELDFILTER) {
        assert(_fieldFilterParser);
        return _fieldFilterParser->parse(data.data(), data.size(), rawDoc);
    }
    BS_LOG(ERROR, "unknown swift data type [%d]!", type);
    return false;
}

void SwiftSchemaBasedRawDocumentParser::setSwiftReader(SwiftReader* reader) { _swiftReader = reader; }

bool SwiftSchemaBasedRawDocumentParser::parseSchemaDocument(const StringView& docString, RawDocument& rawDoc)
{
    ErrorCode ec = ERROR_NONE;
    SchemaReader* schemaReader = _swiftReader->getSchemaReader(docString.data(), ec);
    if (ec != ERROR_NONE) {
        BS_LOG(ERROR, "getSchemaReader fail, ErrorCode [%s]!", ErrorCode_Name(ec).c_str());
        return false;
    }

    if (schemaReader == NULL) {
        BS_LOG(ERROR, "getSchemaReader fail: get NULL!");
        return false;
    }
    const vector<SchemaReaderField>& fields = schemaReader->parseFromString(docString.data(), docString.size());
    for (const auto& field : fields) {
        if (field.isNone) {
            continue;
        }
        rawDoc.setField(field.key, field.value);
        // TODO: parse subFields as needed
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

DataType SwiftSchemaBasedRawDocumentParser::parseDataType(const string& docString)
{
    assert(docString.size() >= sizeof(uint32_t));
    uint32_t typeValue = *(uint32_t*)docString.data();
    return DataType(typeValue);
}

StringView SwiftSchemaBasedRawDocumentParser::parseData(const string& docString)
{
    assert(docString.size() >= sizeof(uint32_t));
    char* data = (char*)docString.data() + sizeof(uint32_t);
    size_t len = docString.size() - sizeof(uint32_t);
    return StringView(data, len);
}

}} // namespace build_service::reader
