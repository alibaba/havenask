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
#include "ha3/sql/framework/SqlResultUtil.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <flatbuffers/stl_emulation.h>
#include <memory>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/proto/SqlResult_generated.h"
#include "ha3/proto/TwoDimTable_generated.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/Row.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace isearch::fbs;
using namespace autil::mem_pool;
using namespace flatbuffers;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, SqlResultUtil);
static const uint32_t FB_BUILDER_INIT_SIZE = 1024 * 1024;

SqlResultUtil::SqlResultUtil(bool useByteString)
    : _useByteString(useByteString) {}

SqlResultUtil::~SqlResultUtil() {}

void SqlResultUtil::toFlatBuffersString(double processTime,
                                        uint32_t rowCount,
                                        double coveredPercent,
                                        const string &searchInfo,
                                        QrsSessionSqlResult &sqlResult,
                                        const map<string, bool> &leaderInfo,
                                        const map<string, int64_t> &watermarkInfo,
                                        bool hasSoftFailure,
                                        Pool *pool) {
    FlatBufferSimpleAllocator flatbufferAllocator(pool);
    FlatBufferBuilder fbb(FB_BUILDER_INIT_SIZE, &flatbufferAllocator);

    Offset<SqlResult> fbSqlResult = CreateFBSqlResult(processTime,
                                                      rowCount,
                                                      coveredPercent,
                                                      searchInfo,
                                                      sqlResult,
                                                      leaderInfo,
                                                      watermarkInfo,
                                                      hasSoftFailure,
                                                      fbb);
    fbb.Finish(fbSqlResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    sqlResult.resultStr.assign(data, fbb.GetSize());
}

Offset<SqlResult> SqlResultUtil::CreateFBSqlResult(double processTime,
                                                   uint32_t rowCount,
                                                   double coveredPercent,
                                                   const string &searchInfo,
                                                   const QrsSessionSqlResult &sqlResult,
                                                   const map<string, bool> &leaderInfo,
                                                   const map<string, int64_t> &watermarkInfo,
                                                   bool hasSoftFailure,
                                                   FlatBufferBuilder &fbb) {
    return isearch::fbs::CreateSqlResult(fbb,
                                         processTime,
                                         rowCount,
                                         CreateSqlErrorResult(sqlResult.errorResult, fbb),
                                         CreateSqlTable(sqlResult.table, fbb),
                                         fbb.CreateString(searchInfo),
                                         coveredPercent,
                                         fbb.CreateVector(CreateLeaderInfo(leaderInfo, fbb)),
                                         fbb.CreateVector(CreateWatermarkInfo(watermarkInfo, fbb)),
                                         hasSoftFailure);
}

bool SqlResultUtil::FromFBSqlResult(const isearch::fbs::SqlResult *fbSqlResult,
                                    ErrorResult &errorResult,
                                    bool &hasSoftFailure,
                                    std::shared_ptr<table::Table> &table) {
    if (!FromSqlErrorResult(fbSqlResult->errorResult(), errorResult)) {
        SQL_LOG(ERROR, "from sql error result failed");
        return false;
    }
    if (!FromSqlTable(fbSqlResult->sqlTable(), table)) {
        SQL_LOG(ERROR, "from sql error result failed");
        return false;
    }
    hasSoftFailure = fbSqlResult->hasSoftFailure() || (fbSqlResult->coveredPercent() < 1 - 1e-5);
    return true;
}

vector<Offset<TableLeaderInfo>> SqlResultUtil::CreateLeaderInfo(const map<string, bool> &leaderInfo,
                                                                FlatBufferBuilder &fbb) {
    vector<Offset<TableLeaderInfo>> offsets;
    for (const auto &info : leaderInfo) {
        offsets.push_back(
            isearch::fbs::CreateTableLeaderInfoDirect(fbb, info.first.c_str(), info.second));
    }
    return offsets;
}

vector<Offset<TableBuildWatermark>>
SqlResultUtil::CreateWatermarkInfo(const map<string, int64_t> &watermarkInfo,
                                   FlatBufferBuilder &fbb) {
    vector<Offset<TableBuildWatermark>> offsets;
    for (const auto &info : watermarkInfo) {
        offsets.emplace_back(
            isearch::fbs::CreateTableBuildWatermarkDirect(fbb, info.first.c_str(), info.second));
    }
    return offsets;
}

Offset<SqlErrorResult> SqlResultUtil::CreateSqlErrorResult(const ErrorResult &errorResult,
                                                           FlatBufferBuilder &fbb) {
    return isearch::fbs::CreateSqlErrorResult(fbb,
                                              fbb.CreateString(errorResult.getPartitionID()),
                                              fbb.CreateString(errorResult.getHostName()),
                                              errorResult.getErrorCode(),
                                              fbb.CreateString(errorResult.getErrorDescription()));
}

bool SqlResultUtil::FromSqlErrorResult(const isearch::fbs::SqlErrorResult *fbSqlErrorResult,
                                       ErrorResult &errorResult) {
    errorResult.setHostInfo(fbSqlErrorResult->partitionId()->str(),
                            fbSqlErrorResult->hostName()->str());
    errorResult.setErrorMsg(fbSqlErrorResult->errorDescription()->str());
    errorResult.setErrorCode(fbSqlErrorResult->errorCode());
    return true;
}

Offset<TwoDimTable> SqlResultUtil::CreateSqlTable(const std::shared_ptr<table::Table> &table,
                                                  FlatBufferBuilder &fbb) {
#define NUMERIC_SWITCH_CASE(fbType, fieldType)                                                     \
    case fieldType: {                                                                              \
        typedef MatchDocBuiltinType2CppType<fieldType, false>::CppType cppType;                    \
        ColumnData<cppType> *columnData = table->getColumn(columnIdx)->getColumnData<cppType>();   \
        if (!columnData) {                                                                         \
            break;                                                                                 \
        }                                                                                          \
        CreateSqlTableColumnBy##fbType(columnName, columnData, fbb, sqlTableColumns);              \
        break;                                                                                     \
    }

#define MULTINUMERIC_SWITCH_CASE(fbType, fieldType)                                                \
    case fieldType: {                                                                              \
        typedef MatchDocBuiltinType2CppType<fieldType, true>::CppType cppType;                     \
        ColumnData<cppType> *columnData = table->getColumn(columnIdx)->getColumnData<cppType>();   \
        if (!columnData) {                                                                         \
            break;                                                                                 \
        }                                                                                          \
        CreateSqlTableColumnByMulti##fbType(columnName, columnData, fbb, sqlTableColumns);         \
        break;                                                                                     \
    }

    vector<Offset<isearch::fbs::Column>> sqlTableColumns;
    size_t rowCount = 0u;
    if (table) {
        rowCount = table->getRowCount();
        size_t columnCount = table->getColumnCount();
        for (size_t columnIdx = 0; columnIdx < columnCount; columnIdx++) {
            const string &columnName = table->getColumnName(columnIdx);
            matchdoc::ValueType columnValueType = table->getColumnType(columnIdx);
            matchdoc::BuiltinType columnType = columnValueType.getBuiltinType();
            bool isMultiValue = columnValueType.isMultiValue();
            if (!isMultiValue) {
                switch (columnType) {
                    FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(NUMERIC_SWITCH_CASE);
                default:
                    auto column = table->getColumn(columnIdx);
                    if (_useByteString) {
                        CreateSqlTableColumnByByteString(columnName, column, fbb, sqlTableColumns);
                    } else {
                        CreateSqlTableColumnByString(columnName, column, fbb, sqlTableColumns);
                    }
                }
            } else {
                switch (columnType) {
                    FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(MULTINUMERIC_SWITCH_CASE);
                case bt_string: {
                    ColumnData<autil::MultiString> *columnData
                        = table->getColumn(columnIdx)->getColumnData<autil::MultiString>();
                    if (_useByteString) {
                        CreateSqlTableColumnByMultiByteString(
                            columnName, columnData, fbb, sqlTableColumns);
                    } else {
                        CreateSqlTableColumnByMultiString(
                            columnName, columnData, fbb, sqlTableColumns);
                    }
                    break;
                }
                default: {
                    auto column = table->getColumn(columnIdx);
                    if (_useByteString) {
                        CreateSqlTableColumnByByteString(columnName, column, fbb, sqlTableColumns);
                    } else {
                        CreateSqlTableColumnByString(columnName, column, fbb, sqlTableColumns);
                    }
                }
                }
            }
        }
    }

#undef NUMERIC_SWITCH_CASE
#undef MULTINUMERIC_SWITCH_CASE

    return CreateTwoDimTable(fbb, (uint32_t)rowCount, fbb.CreateVector(sqlTableColumns));
}

bool SqlResultUtil::FromSqlTable(const isearch::fbs::TwoDimTable *fbsTable,
                                 std::shared_ptr<table::Table> &table) {
    if (!table) {
        SQL_LOG(ERROR, "table must be created");
        return false;
    }
    const Vector<Offset<isearch::fbs::Column>> *columns = fbsTable->columns();
    table->batchAllocateRow(fbsTable->rowCount());
    for (size_t columnIdx = 0; columnIdx < columns->size(); columnIdx++) {
        const isearch::fbs::Column *fbsColumn = columns->Get(columnIdx);
        const flatbuffers::String *fieldName = fbsColumn->name();
        assert(fieldName && "field name need exist");
        auto columnName = fieldName->str();
        switch (fbsColumn->value_type()) {
#define NUMERIC_SWITCH_CASE(fbType, fieldType)                                                     \
    case ColumnType_##fbType##Column: {                                                            \
        auto columnData = TableUtil::declareAndGetColumnData<                                      \
            matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType, false>::CppType>(           \
            table, columnName, true, true);                                                        \
        if (!columnData) {                                                                         \
            SQL_LOG(ERROR, "duplicate column name [%s]", columnName.c_str());                      \
            return false;                                                                          \
        }                                                                                          \
        if (!FromSqlTableColumnBy##fbType(table->getDataPool(), fbsColumn, columnData)) {          \
            SQL_LOG(ERROR, "from sql table column failed, name[%s]", columnName.c_str());          \
            return false;                                                                          \
        }                                                                                          \
        break;                                                                                     \
    }

#define MULTINUMERIC_SWITCH_CASE(fbType, fieldType)                                                \
    case ColumnType_Multi##fbType##Column: {                                                       \
        auto columnData = TableUtil::declareAndGetColumnData<                                      \
            matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType, true>::CppType>(            \
            table, columnName, true, true);                                                        \
        if (!columnData) {                                                                         \
            SQL_LOG(ERROR, "duplicate column name [%s]", columnName.c_str());                      \
            return false;                                                                          \
        }                                                                                          \
        if (!FromSqlTableColumnByMulti##fbType(table->getDataPool(), fbsColumn, columnData)) {     \
            SQL_LOG(ERROR, "from sql table column failed, name[%s]", columnName.c_str());          \
            return false;                                                                          \
        }                                                                                          \
        break;                                                                                     \
    }
            FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(NUMERIC_SWITCH_CASE)
            FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(MULTINUMERIC_SWITCH_CASE)
            NUMERIC_SWITCH_CASE(String, bt_string)
            NUMERIC_SWITCH_CASE(ByteString, bt_string)
            MULTINUMERIC_SWITCH_CASE(String, bt_string)
            MULTINUMERIC_SWITCH_CASE(ByteString, bt_string)
        default:
            SQL_LOG(ERROR,
                    "unsupported column type[%d] column[%s]",
                    fbsColumn->value_type(),
                    fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
            return false;
        }
    }

#undef NUMERIC_SWITCH_CASE
#undef MULTINUMERIC_SWITCH_CASE

    return true;
}

#define IMPL_CREATE_SQL_TABLE_COLUMN_BY_TYPE(fbType, fieldType)                                    \
    using fieldType##type                                                                          \
        = matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType, false>::CppType;              \
    void SqlResultUtil::CreateSqlTableColumnBy##fbType(                                            \
        const string &columnName,                                                                  \
        ColumnData<fieldType##type> *columnData,                                                   \
        FlatBufferBuilder &fbb,                                                                    \
        vector<Offset<isearch::fbs::Column>> &sqlTableColumns) {                                   \
        size_t rowCount = columnData->getRowCount();                                               \
        vector<fieldType##type> columnValue(rowCount, 0);                                          \
        for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {                                     \
            columnValue[rowIdx] = columnData->get(rowIdx);                                         \
        }                                                                                          \
        Offset<fbType##Column> fb_columnValue                                                      \
            = Create##fbType##Column(fbb, fbb.CreateVector(columnValue));                          \
        sqlTableColumns.emplace_back(CreateColumnDirect(                                           \
            fbb, columnName.c_str(), ColumnType_##fbType##Column, fb_columnValue.Union()));        \
    }                                                                                              \
    bool SqlResultUtil::FromSqlTableColumnBy##fbType(                                              \
        autil::mem_pool::Pool *pool,                                                               \
        const isearch::fbs::Column *fbsColumn,                                                     \
        table::ColumnData<matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType,               \
                                                                false>::CppType> *columnData) {    \
        const auto *column = fbsColumn->value_as_##fbType##Column();                               \
        if (!column) {                                                                             \
            SQL_LOG(ERROR,                                                                         \
                    "fbs column type cast to [%s] failed, column[%s]",                             \
                    #fbType,                                                                       \
                    fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");                    \
            return false;                                                                          \
        }                                                                                          \
        const auto *columnValue = column->value();                                                 \
        if (columnValue->size() != columnData->getRowCount()) {                                    \
            SQL_LOG(ERROR,                                                                         \
                    "row count mismatch, fb[%u] table[%lu], column[%s]",                           \
                    columnValue->size(),                                                           \
                    columnData->getRowCount(),                                                     \
                    fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");                    \
            return false;                                                                          \
        }                                                                                          \
        for (size_t i = 0; i < columnValue->size(); ++i) {                                         \
            columnData->set(i, columnValue->Get(i));                                               \
        }                                                                                          \
        return true;                                                                               \
    }

#define IMPL_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(fbType, fieldType)                               \
    using multi##fieldType##type                                                                   \
        = matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType, true>::CppType;               \
    void SqlResultUtil::CreateSqlTableColumnByMulti##fbType(                                       \
        const string &columnName,                                                                  \
        ColumnData<multi##fieldType##type> *columnData,                                            \
        FlatBufferBuilder &fbb,                                                                    \
        vector<Offset<isearch::fbs::Column>> &sqlTableColumns) {                                   \
        vector<Offset<isearch::fbs::Multi##fbType>> columnValue;                                   \
        size_t rowCount = columnData->getRowCount();                                               \
        for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {                                     \
            multi##fieldType##type *value = columnData->getPointer(rowIdx);                        \
            columnValue.push_back(CreateMulti##fbType(                                             \
                fbb,                                                                               \
                fbb.CreateVector<multi##fieldType##type::type>(value->data(), value->size())));    \
        }                                                                                          \
        Offset<Multi##fbType##Column> fb_columnValue                                               \
            = CreateMulti##fbType##Column(fbb, fbb.CreateVector(columnValue));                     \
        sqlTableColumns.emplace_back(CreateColumnDirect(                                           \
            fbb, columnName.c_str(), ColumnType_Multi##fbType##Column, fb_columnValue.Union()));   \
    }                                                                                              \
    bool SqlResultUtil::FromSqlTableColumnByMulti##fbType(                                         \
        autil::mem_pool::Pool *pool,                                                               \
        const isearch::fbs::Column *fbsColumn,                                                     \
        table::ColumnData<multi##fieldType##type> *columnData) {                                   \
        const isearch::fbs::Multi##fbType##Column *column                                          \
            = fbsColumn->value_as_Multi##fbType##Column();                                         \
        if (!column) {                                                                             \
            SQL_LOG(ERROR,                                                                         \
                    "fbs column type cast to [Multi%s] failed, column[%s]",                        \
                    #fbType,                                                                       \
                    fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");                    \
            return false;                                                                          \
        }                                                                                          \
        const flatbuffers::Vector<flatbuffers::Offset<isearch::fbs::Multi##fbType>> *columnValue   \
            = column->value();                                                                     \
        if (columnValue->size() != columnData->getRowCount()) {                                    \
            SQL_LOG(ERROR,                                                                         \
                    "row count mismatch, fb[%u] table[%lu], column[%s]",                           \
                    columnValue->size(),                                                           \
                    columnData->getRowCount(),                                                     \
                    fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");                    \
            return false;                                                                          \
        }                                                                                          \
        for (size_t i = 0; i < columnValue->size(); ++i) {                                         \
            const isearch::fbs::Multi##fbType *fbValue = columnValue->Get(i);                      \
            auto *buf = MultiValueCreator::createMultiValueBuffer(                                 \
                fbValue->value()->data(), fbValue->value()->size(), pool);                         \
            multi##fieldType##type mv(buf);                                                        \
            columnData->set(i, mv);                                                                \
        }                                                                                          \
        return true;                                                                               \
    }

void SqlResultUtil::CreateSqlTableColumnByString(
    const string &columnName,
    table::Column *column,
    FlatBufferBuilder &fbb,
    vector<Offset<isearch::fbs::Column>> &sqlTableColumns) {
    auto *columnData = column->getColumnData<autil::MultiChar>();
    assert(columnData != nullptr);
    vector<Offset<flatbuffers::String>> columnValue;
    size_t rowCount = columnData->getRowCount();
    for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto mc = columnData->get(rowIdx);
        columnValue.emplace_back(fbb.CreateString(mc.data(), mc.size()));
    }
    Offset<StringColumn> fb_columnValue = CreateStringColumn(fbb, fbb.CreateVector(columnValue));
    sqlTableColumns.emplace_back(CreateColumnDirect(
        fbb, columnName.c_str(), ColumnType_StringColumn, fb_columnValue.Union()));
}

bool SqlResultUtil::FromSqlTableColumnByString(autil::mem_pool::Pool *pool,
                                               const isearch::fbs::Column *fbsColumn,
                                               table::ColumnData<autil::MultiChar> *columnData) {
    const auto *column = fbsColumn->value_as_StringColumn();
    if (!column) {
        SQL_LOG(ERROR,
                "fbs column type cast to [%s] failed, column[%s]",
                "String",
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    const auto *columnValue = column->value();
    if (columnValue->size() != columnData->getRowCount()) {
        SQL_LOG(ERROR,
                "row count mismatch, fb[%u] table[%lu], column[%s]",
                columnValue->size(),
                columnData->getRowCount(),
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    for (size_t i = 0; i < columnValue->size(); ++i) {
        const flatbuffers::String *strValue = columnValue->Get(i);
        assert(strValue && "column value need exist");
        auto buf = MultiValueCreator::createMultiValueBuffer(strValue->c_str(), strValue->size(), pool);
        MultiChar ms(buf);
        columnData->set(i, ms);
    }
    return true;
}

void SqlResultUtil::CreateSqlTableColumnByMultiString(
    const string &columnName,
    ColumnData<autil::MultiString> *columnData,
    FlatBufferBuilder &fbb,
    vector<Offset<isearch::fbs::Column>> &sqlTableColumns) {
    vector<Offset<isearch::fbs::MultiString>> columnValue;
    size_t rowCount = columnData->getRowCount();
    for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        autil::MultiString *value = columnData->getPointer(rowIdx);
        vector<string> strVec;
        for (size_t i = 0; i < value->size(); i++) {
            MultiChar tmpStr = (*value)[i];
            strVec.push_back(string(tmpStr.data(), tmpStr.size()));
        }
        columnValue.push_back(CreateMultiString(fbb, fbb.CreateVectorOfStrings(strVec)));
    }
    Offset<MultiStringColumn> fb_columnValue
        = CreateMultiStringColumn(fbb, fbb.CreateVector(columnValue));
    sqlTableColumns.emplace_back(CreateColumnDirect(
        fbb, columnName.c_str(), ColumnType_MultiStringColumn, fb_columnValue.Union()));
}

bool SqlResultUtil::FromSqlTableColumnByMultiString(
    autil::mem_pool::Pool *pool,
    const isearch::fbs::Column *fbsColumn,
    table::ColumnData<autil::MultiString> *columnData) {
    const auto *column = fbsColumn->value_as_MultiStringColumn();
    if (!column) {
        SQL_LOG(ERROR,
                "fbs column type cast to [Multi%s] failed, column[%s]",
                "MultiString",
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    const auto *columnValue = column->value();
    if (columnValue->size() != columnData->getRowCount()) {
        SQL_LOG(ERROR,
                "row count mismatch, fb[%u] table[%lu], column[%s]",
                columnValue->size(),
                columnData->getRowCount(),
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    std::vector<StringView> strVec;
    for (size_t i = 0; i < columnValue->size(); ++i) {
        auto offset = columnValue->Get(i);
        strVec.clear();
        for (auto value : *offset->value()) {
            strVec.emplace_back(value->c_str(), value->size());
        }
        auto *buf = MultiValueCreator::createMultiStringBuffer(strVec, pool);
        autil::MultiString ms(buf);
        columnData->set(i, ms);
    }
    return true;
}

void SqlResultUtil::CreateSqlTableColumnByByteString(
    const string &columnName,
    table::Column *column,
    FlatBufferBuilder &fbb,
    vector<Offset<isearch::fbs::Column>> &sqlTableColumns) {
    auto *columnData = column->getColumnData<autil::MultiChar>();
    assert(columnData != nullptr);
    vector<Offset<isearch::fbs::ByteString>> columnValue;
    size_t rowCount = columnData->getRowCount();
    for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto mc = columnData->get(rowIdx);
        columnValue.emplace_back(
            CreateByteString(fbb, fbb.CreateVector<int8_t>((int8_t *)mc.data(), mc.size())));
    }
    Offset<ByteStringColumn> fb_columnValue
        = CreateByteStringColumn(fbb, fbb.CreateVector(columnValue));
    sqlTableColumns.emplace_back(CreateColumnDirect(
        fbb, columnName.c_str(), ColumnType_ByteStringColumn, fb_columnValue.Union()));
}

bool SqlResultUtil::FromSqlTableColumnByByteString(
    autil::mem_pool::Pool *pool,
    const isearch::fbs::Column *fbsColumn,
    table::ColumnData<autil::MultiChar> *columnData) {
    const auto *column = fbsColumn->value_as_ByteStringColumn();
    if (!column) {
        SQL_LOG(ERROR,
                "fbs column type cast to [%s] failed, column[%s]",
                "ByteString",
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    const auto *columnValue = column->value();
    if (columnValue->size() != columnData->getRowCount()) {
        SQL_LOG(ERROR,
                "row count mismatch, fb[%u] table[%lu], column[%s]",
                columnValue->size(),
                columnData->getRowCount(),
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    for (size_t i = 0; i < columnValue->size(); ++i) {
        auto byteVal = columnValue->Get(i)->value();
        auto buf = MultiValueCreator::createMultiValueBuffer(byteVal->data(), byteVal->size(), pool);
        MultiChar ms(buf);
        columnData->set(i, ms);
    }
    return true;
}

void SqlResultUtil::CreateSqlTableColumnByMultiByteString(
    const string &columnName,
    ColumnData<autil::MultiString> *columnData,
    FlatBufferBuilder &fbb,
    vector<Offset<isearch::fbs::Column>> &sqlTableColumns) {
    vector<Offset<isearch::fbs::MultiByteString>> columnValue;
    size_t rowCount = columnData->getRowCount();
    for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        autil::MultiString *value = columnData->getPointer(rowIdx);
        vector<Offset<isearch::fbs::ByteString>> strVec;
        for (size_t i = 0; i < value->size(); i++) {
            MultiChar tmpStr = (*value)[i];
            strVec.push_back(CreateByteString(
                fbb, fbb.CreateVector<int8_t>((int8_t *)tmpStr.data(), tmpStr.size())));
        }
        columnValue.push_back(CreateMultiByteString(fbb, fbb.CreateVector(strVec)));
    }
    Offset<MultiByteStringColumn> fb_columnValue
        = CreateMultiByteStringColumn(fbb, fbb.CreateVector(columnValue));
    sqlTableColumns.emplace_back(CreateColumnDirect(
        fbb, columnName.c_str(), ColumnType_MultiByteStringColumn, fb_columnValue.Union()));
}

bool SqlResultUtil::FromSqlTableColumnByMultiByteString(
    autil::mem_pool::Pool *pool,
    const isearch::fbs::Column *fbsColumn,
    table::ColumnData<autil::MultiString> *columnData) {
    const isearch::fbs::MultiByteStringColumn *column = fbsColumn->value_as_MultiByteStringColumn();
    if (!column) {
        SQL_LOG(ERROR,
                "fbs column type cast to [Multi%s] failed, column[%s]",
                "MultiByteString",
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    const flatbuffers::Vector<flatbuffers::Offset<MultiByteString>> *columnValue = column->value();
    if (columnValue->size() != columnData->getRowCount()) {
        SQL_LOG(ERROR,
                "row count mismatch, fb[%u] table[%lu], column[%s]",
                columnValue->size(),
                columnData->getRowCount(),
                fbsColumn->name() ? fbsColumn->name()->str().c_str() : "");
        return false;
    }
    std::vector<StringView> strVec;
    for (size_t i = 0; i < columnValue->size(); ++i) {
        const isearch::fbs::MultiByteString *fbValue = columnValue->Get(i);
        strVec.clear();
        for (const isearch::fbs::ByteString *byteString : *fbValue->value()) {
            strVec.emplace_back((const char *)byteString->value()->data(), byteString->value()->size());
        }
        auto *buf = MultiValueCreator::createMultiStringBuffer(strVec, pool);
        autil::MultiString ms(buf);
        columnData->set(i, ms);
    }
    return true;
}


FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(IMPL_CREATE_SQL_TABLE_COLUMN_BY_TYPE)
FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(IMPL_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE)

#undef IMPL_CREATE_SQL_TABLE_COLUMN_BY_TYPE
#undef IMPL_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE

} // namespace sql
} // namespace isearch
