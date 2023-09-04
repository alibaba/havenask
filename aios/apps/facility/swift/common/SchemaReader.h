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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/Span.h"
#include "swift/common/Common.h"
#include "swift/common/FieldSchema.h"

namespace swift {
namespace common {

struct SRFieldMeta {
    SRFieldMeta() {}
    SRFieldMeta(const std::string &name) : key(name.c_str(), name.size()) {}

    autil::StringView key;
    std::vector<autil::StringView> subKeys;
};

struct SchemaReaderField {
    SchemaReaderField() : isNone(true){};
    SchemaReaderField(const autil::StringView &key_, const autil::StringView &value_, bool isNone_ = false)
        : isNone(isNone_), key(key_), value(value_) {}

    bool isNone;
    autil::StringView key;
    autil::StringView value;
    std::vector<SchemaReaderField> subFields;
};

class SchemaReader {
public:
    SchemaReader();
    ~SchemaReader();

private:
    SchemaReader(const SchemaReader &);
    SchemaReader &operator=(const SchemaReader &);

public:
    bool loadSchema(const std::string &schemaStr);
    std::vector<SchemaReaderField> parseFromString(const char *dataStr, size_t dataLen);
    std::vector<SchemaReaderField> parseFromString(const std::string &encodeStr);
    static bool readVersion(const char *data, int32_t &version);

private:
    std::vector<SchemaReaderField>
    parseFromString(const char *dataStr, size_t dataLen, SchemaHeader &header); // for test
    void generateFieldMeta();
    bool readLength(uint32_t &readCursor, uint32_t dataLength, const char *data, uint32_t &value);
    bool readBytes(uint32_t &readCursor, uint32_t dataLength, const char *data, uint32_t len, autil::StringView &cs);
    bool readBool(uint32_t &readCursor, uint32_t dataLength, const char *data, bool &isNone);
    bool readHeader(const char *data, size_t len, SchemaHeader &header);

private:
    std::vector<SRFieldMeta> _fieldMeta;
    FieldSchema _schema;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SchemaReader);

} // namespace common
} // namespace swift
