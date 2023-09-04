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
#include "swift/common/SchemaReader.h"

#include <cstddef>
#include <memory>

#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, SchemaReader);

SchemaReader::SchemaReader() {}

SchemaReader::~SchemaReader() {}

bool SchemaReader::loadSchema(const std::string &schemaStr) {
    _schema.clear();
    if (!_schema.fromJsonString(schemaStr)) {
        AUTIL_LOG(ERROR, "load schema error[%s]", schemaStr.c_str());
        return false;
    }
    if (_schema.topicName.empty()) {
        AUTIL_LOG(ERROR, "parse schema error, topic not set");
        return false;
    }
    if (_schema.fields.empty()) {
        AUTIL_LOG(ERROR, "parse schema error, fields not set");
        return false;
    }
    generateFieldMeta();
    return true;
}

void SchemaReader::generateFieldMeta() {
    _fieldMeta.clear();
    _fieldMeta.reserve(_schema.fields.size());
    for (const auto &field : _schema.fields) {
        SRFieldMeta meta(field.name);
        for (const auto &sub : field.subFields) {
            meta.subKeys.emplace_back(StringView(sub.name.c_str(), sub.name.size()));
        }
        _fieldMeta.emplace_back(meta);
    }
}

std::vector<SchemaReaderField> SchemaReader::parseFromString(const string &encodeStr) {
    return parseFromString(encodeStr.c_str(), encodeStr.size());
}

std::vector<SchemaReaderField> SchemaReader::parseFromString(const char *data, size_t dataLength) {
    SchemaHeader header;
    return parseFromString(data, dataLength, header);
}

std::vector<SchemaReaderField>
SchemaReader::parseFromString(const char *data, size_t dataLength, SchemaHeader &header) {
    std::vector<SchemaReaderField> fieldVec;
    if (NULL == data || 0 == dataLength) {
        return fieldVec;
    }
    uint32_t readCursor = 0;
    uint32_t len = 0;
    StringView value;

    fieldVec.reserve(_fieldMeta.size());
    size_t count = 0;
    if (!readHeader(data, dataLength, header)) {
        return fieldVec;
    }
    readCursor += sizeof(SchemaHeader);
    bool isNone = true;
    while (readCursor < dataLength && count < _fieldMeta.size()) {
        SchemaReaderField afield;
        afield.key = _fieldMeta[count].key;
        if ((!readBool(readCursor, dataLength, data, afield.isNone)) && readLength(readCursor, dataLength, data, len)) {
            readBytes(readCursor, dataLength, data, len, afield.value);
        }
        const auto &subKeys = _fieldMeta[count].subKeys;
        for (size_t index = 0; index < subKeys.size(); ++index) {
            if (readBool(readCursor, dataLength, data, isNone)) {
                afield.subFields.emplace_back(subKeys[index], StringView(), true);
                continue;
            }
            if (readLength(readCursor, dataLength, data, len) && readBytes(readCursor, dataLength, data, len, value)) {
                afield.subFields.emplace_back(subKeys[index], value, false);
            }
        }
        fieldVec.emplace_back(afield);
        count++;
    }
    return fieldVec;
}

bool SchemaReader::readLength(uint32_t &readCursor, uint32_t dataLength, const char *data, uint32_t &value) {
    if (readCursor >= dataLength) {
        return false;
    }
    if (!((unsigned char &)data[readCursor] & 0x80)) {
        value = (unsigned char &)data[readCursor++];
        return true;
    }

    uint32_t b = 0;
    uint32_t result = 0;
    for (uint32_t i = 0; i < 5; ++i) {
        if (readCursor >= dataLength) {
            return false;
        }
        b = (unsigned char &)data[readCursor++];
        uint32_t offset = i * 7;
        if (i < 4) {
            result |= (b & 0x7F) << offset;
        } else {
            result |= b << offset;
        }
        if (!(b & 0x80)) {
            value = result;
            return true;
        }
    }

    return false;
}

bool SchemaReader::readBytes(
    uint32_t &readCursor, uint32_t dataLength, const char *data, uint32_t len, StringView &cs) {
    if (readCursor + len > dataLength) {
        return false;
    }
    cs = {data + readCursor, len};
    readCursor += len;
    return true;
}

bool SchemaReader::readBool(uint32_t &readCursor, uint32_t dataLength, const char *data, bool &isNone) {
    if (readCursor >= dataLength) {
        return false;
    }
    uint32_t value = 0;
    if (!readLength(readCursor, dataLength, data, value)) {
        return false;
    }
    isNone = value == 0 ? false : true;
    return isNone;
}

bool SchemaReader::readHeader(const char *data, size_t len, SchemaHeader &header) {
    if (len < sizeof(SchemaHeader)) {
        return false;
    }
    header.fromBuf(data);
    return true;
}

bool SchemaReader::readVersion(const char *data, int32_t &version) {
    if (NULL == data) {
        return false;
    }
    SchemaHeader header;
    header.fromBuf(data);
    version = header.version;
    return 0 != version; // 0 represent latest version, should not in msg
}

} // namespace common
} // namespace swift
