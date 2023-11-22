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
#include "table/TableUtil.h"

#include <algorithm>
#include <iomanip>
#include <ostream>

#include "autil/HashUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "matchdoc/ValueType.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Comparator.h"
#include "table/Table.h"
#include "table/TableJson.h"
#include "table/ValueTypeSwitch.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace autil::legacy;

namespace table {
AUTIL_LOG_SETUP(table, TableUtil);

void TableUtil::sort(const TablePtr &table, Comparator *comparator) {
    assert(table != nullptr);
    assert(comparator);
    vector<Row> rows = table->getRows();
    std::sort(rows.begin(), rows.end(), [&comparator](Row a, Row b) { return comparator->compare(a, b); });
    table->setRows(std::move(rows));
}

void TableUtil::topK(const TablePtr &table, Comparator *comparator, size_t topk, bool reserve) {
    assert(table != nullptr);
    assert(comparator);
    size_t rowCount = table->getRowCount();
    if (topk >= rowCount) {
        return;
    }
    vector<Row> rows = table->getRows();
    nth_element(rows.begin(), rows.begin() + topk, rows.end(), [&comparator](Row a, Row b) {
        return comparator->compare(a, b);
    });
    if (!reserve) {
        rows.resize(topk);
    }
    table->setRows(std::move(rows));
}

void TableUtil::topK(const TablePtr &table, Comparator *comparator, size_t topk, vector<Row> &rowVec) {
    assert(table != nullptr);
    assert(comparator);
    if (topk >= rowVec.size()) {
        return;
    }
    nth_element(rowVec.begin(), rowVec.begin() + topk, rowVec.end(), [&comparator](Row a, Row b) {
        return comparator->compare(a, b);
    });
    rowVec.resize(topk);
}

string TableUtil::toString(const TablePtr &table) { return toString(table, 0, std::numeric_limits<size_t>::max()); }

string TableUtil::toString(const TablePtr &table, size_t maxRowCount) { return toString(table, 0, maxRowCount); }

string TableUtil::toString(const TablePtr &table, size_t rowOffset, size_t maxRowCount) {
    if (table == nullptr) {
        return "";
    }
    ostringstream oss;
    size_t rowCount = 0;
    if (table->getRowCount() >= rowOffset) {
        rowCount = table->getRowCount() - rowOffset;
    }
    rowCount = std::min(rowCount, maxRowCount);
    size_t colCount = table->getColumnCount();
    oss << "total:" << table->getRowCount() << ", rows:[" << rowOffset << ", " << rowOffset + rowCount
        << "), cols:" << colCount << endl;
    oss.flags(ios::right);
    const auto &tableSchema = table->getTableSchema();
    for (size_t col = 0; col < colCount; col++) {
        const auto *columnSchema = tableSchema.getColumn(col);
        oss << setw(30) << columnSchema->getName() + " (" + columnSchema->getTypeName() + ")" + " |";
    }
    oss << "\n";

    for (size_t i = 0; i < rowCount; ++i) {
        for (size_t col = 0; col < colCount; col++) {
            Column *column = table->getColumn(col);
            oss << setw(30) << column->toString(i + rowOffset) + " |";
        }
        if (table->isDeletedRow(i + rowOffset)) {
            oss << " (Del)";
        }
        oss << "\n";
    }
    return oss.str();
}

std::string TableUtil::tailToString(const TablePtr &table, size_t maxRowCount) {
    size_t rowOffset = 0;
    if (table && table->getRowCount() >= maxRowCount) {
        rowOffset = table->getRowCount() - maxRowCount;
    }
    return toString(table, rowOffset, maxRowCount);
}

string TableUtil::rowToString(const TablePtr &table, size_t rowOffset) { return toString(table, rowOffset, 1); }

std::string TableUtil::valueTypeToString(ValueType vt) {
    if (!vt.isBuiltInType())
        return "unknown_type";
    std::string ret = "";
    if (vt.isMultiValue()) {
        ret = "multi_";
    }
    switch (vt.getBuiltinType()) {
    case matchdoc::bt_bool: {
        ret += "bool";
        break;
    }
    case matchdoc::bt_int8: {
        ret += "int8";
        break;
    }
    case matchdoc::bt_int16: {
        ret += "int16";
        break;
    }
    case matchdoc::bt_int32: {
        ret += "int32";
        break;
    }
    case matchdoc::bt_int64: {
        ret += "int64";
        break;
    }
    case matchdoc::bt_uint8: {
        ret += "uint8";
        break;
    }
    case matchdoc::bt_uint16: {
        ret += "uint16";
        break;
    }
    case matchdoc::bt_uint32: {
        ret += "uint32";
        break;
    }
    case matchdoc::bt_uint64: {
        ret += "uint64";
        break;
    }
    case matchdoc::bt_float: {
        ret += "float";
        break;
    }
    case matchdoc::bt_double: {
        ret += "double";
        break;
    }
    case matchdoc::bt_string: {
        ret += "multi_char";
        break;
    }
    default:
        ret = "unknown_type";
    }
    return ret;
}

bool TableUtil::toTableJson(const TablePtr &table, TableJson &tableJson) {
    assert(table != nullptr);
    size_t rowCount = table->getRowCount();
    size_t colCount = table->getColumnCount();
    tableJson.data.resize(rowCount, vector<Any>(colCount));
    tableJson.columnName.resize(colCount);
    tableJson.columnType.resize(colCount);
    for (size_t col = 0; col < colCount; col++) {
        auto column = table->getColumn(col);
        if (!column) {
            AUTIL_LOG(ERROR, "get column [%lu] failed", col);
            return false;
        }
        auto vt = column->getColumnSchema()->getType();
        bool isMulti = vt.isMultiValue();
        switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                                                 \
    case ft: {                                                                                                         \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                                     \
        if (isMulti) {                                                                                                 \
            auto columnData = column->getColumnData<autil::MultiValueType<T>>();                                       \
            if (columnData == nullptr) {                                                                               \
                AUTIL_LOG(ERROR, "get column data [%lu] failed", col);                                                 \
                return false;                                                                                          \
            }                                                                                                          \
            vector<T> valVec;                                                                                          \
            for (size_t row = 0; row < rowCount; row++) {                                                              \
                valVec.clear();                                                                                        \
                auto val = columnData->get(row);                                                                       \
                for (size_t i = 0; i < val.size(); ++i) {                                                              \
                    valVec.emplace_back(val[i]);                                                                       \
                }                                                                                                      \
                tableJson.data[row][col] = ToJson(valVec);                                                             \
            }                                                                                                          \
        } else {                                                                                                       \
            auto columnData = column->getColumnData<T>();                                                              \
            if (columnData == nullptr) {                                                                               \
                AUTIL_LOG(ERROR, "get column data [%lu] failed", col);                                                 \
                return false;                                                                                          \
            }                                                                                                          \
            for (size_t row = 0; row < rowCount; row++) {                                                              \
                T val = columnData->get(row);                                                                          \
                tableJson.data[row][col] = ToJson(val);                                                                \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }
            NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        case bt_string: {
            if (isMulti) {
                auto columnData = column->getColumnData<autil::MultiValueType<autil::MultiChar>>();
                if (columnData == nullptr) {
                    AUTIL_LOG(ERROR, "get column data [%lu] failed", col);
                    return false;
                }
                vector<string> valVec;
                for (size_t row = 0; row < rowCount; row++) {
                    valVec.clear();
                    auto val = columnData->get(row);
                    for (size_t i = 0; i < val.size(); ++i) {
                        valVec.emplace_back(string(val[i].data(), val[i].size()));
                    }
                    tableJson.data[row][col] = ToJson(valVec);
                }
            } else {
                auto columnData = column->getColumnData<autil::MultiChar>();
                if (columnData == nullptr) {
                    AUTIL_LOG(ERROR, "get column data [%lu] failed", col);
                    return false;
                }
                for (size_t row = 0; row < rowCount; row++) {
                    const autil::MultiChar &val = columnData->get(row);
                    tableJson.data[row][col] = ToJson(string(val.data(), val.size()));
                }
            }
            break;
        }
        case bt_bool: {
            if (isMulti) {
                AUTIL_LOG(ERROR, "multi bool type not supported");
                return false;
            } else {
                auto columnData = column->getColumnData<bool>();
                if (columnData == nullptr) {
                    AUTIL_LOG(ERROR, "get column data [%lu] failed", col);
                    return false;
                }
                for (size_t row = 0; row < rowCount; row++) {
                    bool val = columnData->get(row);
                    tableJson.data[row][col] = ToJson(val);
                }
            }
            break;
        }
        default: {
            AUTIL_LOG(ERROR, "impossible reach this branch");
            return false;
        }
        } // switch
    }

    for (size_t col = 0; col < colCount; col++) {
        auto column = table->getColumn(col);
        if (column == nullptr) {
            return false;
        }
        auto columnSchema = column->getColumnSchema();
        tableJson.columnName[col] = columnSchema->getName();
        tableJson.columnType[col] = valueTypeToString(columnSchema->getType());
    }
    return true;
}

string TableUtil::toJsonString(const TablePtr &table) {
    TableJson tableJson;
    if (!TableUtil::toTableJson(table, tableJson)) {
        AUTIL_LOG(ERROR, "to table json faild.");
        return "";
    }
    try {
        return FastToJsonString(tableJson, true);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "format table json failed. exception:[%s]", e.what());
        return "";
    }
}

bool TableUtil::calculateGroupKeyHash(TablePtr table, const vector<string> &groupKeyVec, vector<size_t> &hashValue) {
    size_t rowCount = table->getRowCount();
    hashValue.resize(rowCount, 0);
    for (size_t keyIdx = 0; keyIdx < groupKeyVec.size(); ++keyIdx) {
        const string &key = groupKeyVec[keyIdx];
        auto column = table->getColumn(key);
        if (column == nullptr) {
            AUTIL_LOG(ERROR, "invalid column name [%s]", key.c_str());
            return false;
        }
        auto schema = column->getColumnSchema();
        if (schema == nullptr) {
            AUTIL_LOG(ERROR, "invalid column schema [%s]", key.c_str());
            return false;
        }
        auto vt = schema->getType();
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            if constexpr (std::is_same<T, autil::HllCtx>::value) {
                return false;
            } else {
                ColumnData<T> *columnData = column->getColumnData<T>();
                if (unlikely(!columnData)) {
                    AUTIL_LOG(ERROR, "impossible cast column data failed");
                    return false;
                }
                if (keyIdx == 0) {
                    for (size_t i = 0; i < rowCount; i++) {
                        const auto &data = columnData->get(i);
                        hashValue[i] = autil::HashUtil::calculateHashValue<T>(data);
                    }
                } else {
                    for (size_t i = 0; i < rowCount; i++) {
                        const auto &data = columnData->get(i);
                        autil::HashUtil::combineHash(hashValue[i], data);
                    }
                }
                return true;
            }
        };
        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            AUTIL_LOG(ERROR, "calculate hash for group key[%s] failed", key.c_str());
            return false;
        }
    }
    return true;
}

bool TableUtil::calculateGroupKeyHashWithSpecificRow(TablePtr table,
                                                     const vector<string> &groupKeyVec,
                                                     const vector<Row> &rows,
                                                     vector<size_t> &hashValue) {
    size_t rowCount = rows.size();
    hashValue.resize(rowCount, 0);
    for (size_t keyIdx = 0; keyIdx < groupKeyVec.size(); ++keyIdx) {
        const string &key = groupKeyVec[keyIdx];
        auto column = table->getColumn(key);
        if (column == nullptr) {
            AUTIL_LOG(ERROR, "invalid column name [%s]", key.c_str());
            return false;
        }
        auto schema = column->getColumnSchema();
        if (schema == nullptr) {
            AUTIL_LOG(ERROR, "invalid column schema [%s]", key.c_str());
            return false;
        }
        auto vt = schema->getType();
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            if constexpr (std::is_same<T, autil::HllCtx>::value) {
                return false;
            } else {
                ColumnData<T> *columnData = column->getColumnData<T>();
                if (unlikely(!columnData)) {
                    AUTIL_LOG(ERROR, "impossible cast column data failed");
                    return false;
                }
                if (keyIdx == 0) {
                    for (size_t i = 0; i < rowCount; i++) {
                        const auto &data = columnData->get(rows[i]);
                        hashValue[i] = autil::HashUtil::calculateHashValue<T>(data);
                    }
                } else {
                    for (size_t i = 0; i < rowCount; i++) {
                        const auto &data = columnData->get(rows[i]);
                        autil::HashUtil::combineHash(hashValue[i], data);
                    }
                }
                return true;
            }
        };
        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            AUTIL_LOG(ERROR, "calculate hash for group key[%s] failed", key.c_str());
            return false;
        }
    }
    return true;
}

} // namespace table
