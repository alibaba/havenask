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

#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/common/FieldSchema.h"

namespace swift {
namespace common {

struct SchemaWriterField {
    SchemaWriterField() : isNone(true){};
    bool isNone;
    std::string value;
    std::unordered_map<std::string, SchemaWriterField *> subFields;
};

class SchemaWriter {
public:
    SchemaWriter();
    ~SchemaWriter();

private:
    SchemaWriter(const SchemaWriter &);
    SchemaWriter &operator=(const SchemaWriter &);

public:
    bool loadSchema(const std::string &schemaStr);
    void setVersion(int32_t version);
    int32_t getVersion();
    std::string getSchemaStr() const;
    bool setField(const std::string &name, const std::string &value, bool isNone = false);
    bool
    setSubField(const std::string &name, const std::string &subName, const std::string &value, bool isNone = false);
    std::string toString();
    std::string toStringWithHeader(); // for test
    void reset();

private:
    void generateFieldMap();
    void writeLength(std::string &dataStr, uint32_t value);
    void writeBool(std::string &dataStr, bool value);
    void writeBytes(std::string &dataStr, const std::string &value);
    void writeHeader(std::string &dataStr);

private:
    char _varintBuffer[sizeof(uint32_t) + 1];
    std::unordered_map<std::string, SchemaWriterField> _fieldMap;
    FieldSchema _schema;
    int32_t _version;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SchemaWriter);

} // namespace common
} // namespace swift
