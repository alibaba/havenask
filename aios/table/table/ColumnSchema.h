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

#include <string>

#include "table/Common.h"
#include "table/DataCommon.h"

namespace table {

class ColumnSchema
{
public:
    ColumnSchema(std::string name, ValueType type);
    ~ColumnSchema();
private:
    ColumnSchema(const ColumnSchema &);
    ColumnSchema& operator=(const ColumnSchema &);
public:
    const std::string &getName() const {
        return _name;
    }
    ValueType getType() const {
        return _type;
    }
    bool operator == (const ColumnSchema &other) const {
        return _name == other._name && _type == other._type;
    }
    bool operator != (const ColumnSchema &other) const {
        return !(*this == other);
    }
    std::string toString() const;

private:
    std::string _name;
    ValueType _type;
};

TABLE_TYPEDEF_PTR(ColumnSchema);

}
