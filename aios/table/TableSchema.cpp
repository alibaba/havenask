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

bool TableSchema::addColumn(const ColumnSchemaPtr &columnSchema) {
    const string &name = columnSchema->getName();
    if (_columnIndex.count(name) > 0) {
        AUTIL_LOG(WARN, "column name [%s] already exists.", name.c_str());
        return false;
    }
    _columnIndex.emplace(name, _columns.size());
    _columns.push_back(columnSchema);
    return true;
}

const ColumnSchema *TableSchema::getColumn(const string &name) const {
    auto it = _columnIndex.find(name);
    if (it == _columnIndex.end()) {
        AUTIL_LOG(WARN, "column [%s] does not exist.", name.c_str());
        return nullptr;
    }
    return getColumn(it->second);
}

const ColumnSchema *TableSchema::getColumn(size_t idx) const {
    assert(idx < size());
    return _columns[idx].get();
}

std::string TableSchema::toString() const {
    string str;
    for (const auto &iter : _columns) {
        str += iter->toString() + ";";
    }
    return str;
}

bool TableSchema::operator==(const TableSchema &other) const {
    if (_columns.size() != other._columns.size()) {
        return false;
    }
    for (size_t i = 0; i < _columns.size(); ++i) {
        const auto *thisColumn = _columns[i].get();
        const auto *otherColumn = other.getColumn(thisColumn->getName());
        if (otherColumn == nullptr || *thisColumn != *otherColumn) {
            return false;
        }
    }
    return true;
}

void TableSchema::reset() {
    _columnIndex.clear();
    _columns.clear();
}

bool TableSchema::hasUserTypeColumn() const {
    return std::any_of(_columns.begin(), _columns.end(), [](const auto &col) { return col->isUserType(); });
}

void TableSchema::swap(TableSchema &other) {
    _columns.swap(other._columns);
    _columnIndex.swap(other._columnIndex);
}

} // namespace table
