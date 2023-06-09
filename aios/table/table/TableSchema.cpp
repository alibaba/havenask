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
#include "table/TableSchema.h"

using namespace std;

namespace table {
AUTIL_LOG_SETUP(table, TableSchema);

bool TableSchema::addColumnSchema(const ColumnSchemaPtr &columnSchema) {
    const string &name = columnSchema->getName();
    if (_columnSchema.find(name) != _columnSchema.end()) {
        AUTIL_LOG(WARN, "column name [%s] already exists.", name.c_str());
        return false;
    }
    _columnSchema.insert(pair<string, ColumnSchemaPtr>(name, columnSchema));
    return true;
}

bool TableSchema::eraseColumnSchema(const std::string &name) {
    auto iter = _columnSchema.find(name);
    if (iter == _columnSchema.end()) {
        AUTIL_LOG(WARN, "column [%s] does not exist.", name.c_str());
        return false;
    }
    _columnSchema.erase(iter);
    return true;
}

ColumnSchema* TableSchema::getColumnSchema(const string &name) const {
    auto iter = _columnSchema.find(name);
    if (iter == _columnSchema.end()) {
        AUTIL_LOG(WARN, "column [%s] does not exist.", name.c_str());
        return NULL;
    }
    return iter->second.get();
}

std::string TableSchema::toString() const {
    string str;
    for (auto iter :_columnSchema) {
        str += iter.second->toString() + ";";
    }
    return str;
}

bool TableSchema::hasMultiValueColumn() const {
    for (const auto &iter : _columnSchema) {
        auto columnSchema = iter.second;
        if (columnSchema->getType().isMultiValue()) {
            return true;
        }
        if (columnSchema->getType().getBuiltinType() == matchdoc::bt_string) {
            return true;
        }
    }
    return false;
}

bool TableSchema::operator == (const TableSchema& other) const {
    if (_columnSchema.size() != other._columnSchema.size()) {
        return false;
    }
    auto iter1 = _columnSchema.begin();
    auto iter2 = other._columnSchema.begin();
    while (iter1 != _columnSchema.end() && iter2 != other._columnSchema.end()) {
        if (iter1->first != iter2->first || *(iter1->second) != *(iter2->second)) {
            return false;
        }
        iter1++;
        iter2++;
    }
    return true;
}

}
