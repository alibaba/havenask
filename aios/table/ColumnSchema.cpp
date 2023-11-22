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
#include "table/ColumnSchema.h"

#include "table/TableUtil.h"

using namespace std;

namespace table {

ColumnSchema::ColumnSchema() = default;

ColumnSchema::ColumnSchema(std::string name, ValueType type) : _type(type), _name(std::move(name)) {
    if (!isUserType()) {
        _typeName = TableUtil::valueTypeToString(_type);
    } else {
        _typeName = "unknown";
    }
}

ColumnSchema::ColumnSchema(std::string name, ValueType type, std::string typeName)
    : _type(type), _name(std::move(name)), _typeName(std::move(typeName)) {}

ColumnSchema::ColumnSchema(const ColumnSchema &other)
    : _type(other._type), _name(other._name), _typeName(other._typeName) {}

ColumnSchema::ColumnSchema(ColumnSchema &&other)
    : _type(other._type), _name(std::move(other._name)), _typeName(std::move(other._typeName)) {}

ColumnSchema::~ColumnSchema() = default;

ColumnSchema &ColumnSchema::operator=(const ColumnSchema &other) {
    _type = other._type;
    _name = other._name;
    _typeName = other._typeName;
    return *this;
}

ColumnSchema &ColumnSchema::operator=(ColumnSchema &&other) {
    _type = other._type;
    _name = std::move(other._name);
    _typeName = std::move(other._typeName);
    return *this;
}

std::string ColumnSchema::toString() const { return "name:" + _name + ",type:" + _typeName; }

void ColumnSchema::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_name);
    dataBuffer.write(_type.getType());
    if (isUserType()) {
        dataBuffer.write(_typeName);
    }
}

void ColumnSchema::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_name);
    uint32_t type = 0;
    dataBuffer.read(type);
    _type.setType(type);
    if (isUserType()) {
        dataBuffer.read(_typeName);
    } else {
        _typeName = TableUtil::valueTypeToString(_type);
    }
}

} // namespace table
