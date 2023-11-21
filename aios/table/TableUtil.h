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
#include <assert.h>
#include <functional>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "table/Column.h"
#include "table/Row.h"
#include "table/Table.h"

namespace table {
class Comparator;
class TableJson;
template <typename T>
class ColumnData;
} // namespace table

namespace table {

class TableUtil {
public:
    static void sort(const TablePtr &table, Comparator *comparator);
    static void topK(const TablePtr &table, Comparator *comparator, size_t topk, bool reserve = false);
    static void topK(const TablePtr &table, Comparator *comparator, size_t topk, std::vector<Row> &rowVec);
    static std::string toString(const TablePtr &table);
    static std::string toString(const TablePtr &table, size_t maxRowCount);
    static std::string toString(const TablePtr &table, size_t rowOffset, size_t maxRowCount);
    static std::string tailToString(const TablePtr &table, size_t maxRowCount);
    static std::string rowToString(const TablePtr &table, size_t rowOffset);
    static std::string toJsonString(const TablePtr &table);
    static bool toTableJson(const TablePtr &table, TableJson &tableJson);
    template <typename T>
    static ColumnData<T> *getColumnData(const TablePtr &table, const std::string &name);
    template <typename T>
    static ColumnData<T> *
    declareAndGetColumnData(const TablePtr &table, const std::string &name, bool existAsError = true);
    static std::string valueTypeToString(ValueType vt);
    static bool calculateGroupKeyHash(table::TablePtr table,
                                      const std::vector<std::string> &groupKeyVec,
                                      std::vector<size_t> &hashValue);
    static bool calculateGroupKeyHashWithSpecificRow(table::TablePtr table,
                                                     const std::vector<std::string> &groupKeyVec,
                                                     const std::vector<table::Row> &rows,
                                                     std::vector<size_t> &hashValue);

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
ColumnData<T> *TableUtil::getColumnData(const TablePtr &table, const std::string &name) {
    assert(table != nullptr);
    auto column = table->getColumn(name);
    if (column == nullptr) {
        AUTIL_LOG(WARN, "column [%s] does not exist", name.c_str());
        return nullptr;
    }
    ColumnData<T> *ret = column->getColumnData<T>();
    if (ret == nullptr) {
        AUTIL_LOG(WARN, "column data [%s] type mismatch", name.c_str());
        return nullptr;
    }
    return ret;
}

template <typename T>
ColumnData<T> *TableUtil::declareAndGetColumnData(const TablePtr &table, const std::string &name, bool existAsError) {
    assert(table != nullptr);
    if (table->getColumn(name) != nullptr) {
        if (existAsError) {
            AUTIL_LOG(WARN, "duplicate column name [%s]", name.c_str());
            return nullptr;
        } else {
            return getColumnData<T>(table, name);
        }
    }
    Column *column = table->declareColumn<T>(name);
    if (column == nullptr) {
        AUTIL_LOG(WARN, "declare column [%s] failed", name.c_str());
        return nullptr;
    }
    return column->template getColumnData<T>();
}

} // namespace table
