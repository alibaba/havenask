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

#include <map>
#include <string>

#include "table/Common.h"
#include "table/ColumnSchema.h"

namespace table {

class TableSchema
{
public:
    TableSchema() {}
    ~TableSchema() {}
private:
    TableSchema(const TableSchema &);
    TableSchema& operator=(const TableSchema &);
public:
    bool addColumnSchema(const ColumnSchemaPtr &columnSchema);
    // bool addColumnSchema(const std::string &name, ValueType vt);
    bool eraseColumnSchema(const std::string &name);
    ColumnSchema* getColumnSchema(const std::string &name) const;
    std::string toString() const;
    bool operator == (const TableSchema& other) const;
    bool hasMultiValueColumn() const;
private:
    std::map<std::string, ColumnSchemaPtr> _columnSchema;
private:
    AUTIL_LOG_DECLARE();
};

TABLE_TYPEDEF_PTR(TableSchema);

}

