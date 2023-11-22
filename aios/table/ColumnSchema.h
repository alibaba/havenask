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

#include <memory>
#include <string>

#include "autil/DataBuffer.h"
#include "matchdoc/ValueType.h"

namespace table {
using ValueType = matchdoc::ValueType;

class ColumnSchema {
public:
    ColumnSchema();
    ColumnSchema(std::string name, ValueType type);
    ColumnSchema(std::string name, ValueType type, std::string typeName);
    ColumnSchema(const ColumnSchema &other);
    ColumnSchema(ColumnSchema &&other);
    ~ColumnSchema();

public:
    ColumnSchema &operator=(const ColumnSchema &other);
    ColumnSchema &operator=(ColumnSchema &&other);

public:
    const std::string &getName() const { return _name; }
    ValueType getType() const { return _type; }
    const std::string &getTypeName() const { return _typeName; }

    bool isUserType() const { return !_type.isBuiltInType(); }

    bool operator==(const ColumnSchema &other) const {
        return _name == other._name && _type.getTypeIgnoreConstruct() == other._type.getTypeIgnoreConstruct() &&
               _typeName == other._typeName;
    }
    bool operator!=(const ColumnSchema &other) const { return !(*this == other); }

    std::string toString() const;

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

private:
    ValueType _type;
    std::string _name;
    std::string _typeName;
};

using ColumnSchemaPtr = std::shared_ptr<ColumnSchema>;

} // namespace table
