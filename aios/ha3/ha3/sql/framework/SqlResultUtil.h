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

#include <flatbuffers/flatbuffers.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/framework/ErrorResult.h"
#include "ha3/sql/framework/QrsSessionSqlResult.h"
#include "table/Table.h"

namespace isearch {
namespace fbs {
struct Column;
struct SqlErrorResult;
struct SqlResult;
struct TwoDimTable;
struct TableLeaderInfo;
struct TableBuildWatermark;

} // namespace fbs
} // namespace isearch
namespace table {
class Column;
template <typename T>
class ColumnData;
} // namespace table

namespace isearch {
namespace sql {

struct QrsSessionSqlRequest;


class FlatBufferSimpleAllocator : public flatbuffers::Allocator {
public:
    explicit FlatBufferSimpleAllocator(autil::mem_pool::Pool *pool)
        : pool_(pool) {}
    uint8_t *allocate(size_t size) override {
        return reinterpret_cast<uint8_t *>(pool_->allocate(size));
    }
    void deallocate(uint8_t *p, size_t size) override {}

private:
    autil::mem_pool::Pool *pool_;
};

#define FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(MY_MACRO)                                            \
    MY_MACRO(UInt8, bt_uint8);                                                                     \
    MY_MACRO(Int8, bt_int8);                                                                       \
    MY_MACRO(UInt16, bt_uint16);                                                                   \
    MY_MACRO(Int16, bt_int16);                                                                     \
    MY_MACRO(UInt32, bt_uint32);                                                                   \
    MY_MACRO(Int32, bt_int32);                                                                     \
    MY_MACRO(UInt64, bt_uint64);                                                                   \
    MY_MACRO(Int64, bt_int64);                                                                     \
    MY_MACRO(Float, bt_float);                                                                     \
    MY_MACRO(Double, bt_double);

class SqlResultUtil {
public:
    SqlResultUtil(bool useByteStringCol = false);
    virtual ~SqlResultUtil();

private:
    SqlResultUtil(const SqlResultUtil &);
    SqlResultUtil &operator=(const SqlResultUtil &);

public:
    void toFlatBuffersString(double processTime,
                             uint32_t rowCount,
                             double coveredPercent,
                             const std::string &searchInfo,
                             QrsSessionSqlResult &sqlResult,
                             const std::map<std::string, bool> &leaderInfo,
                             const std::map<std::string, int64_t> &watermarkInfo,
                             bool hasSoftFailure,
                             autil::mem_pool::Pool *pool);

    flatbuffers::Offset<isearch::fbs::SqlResult>
    CreateFBSqlResult(double processTime,
                      uint32_t rowCount,
                      double coveredPercent,
                      const std::string &searchInfo,
                      const QrsSessionSqlResult &sqlResult,
                      const std::map<std::string, bool> &leaderInfo,
                      const std::map<std::string, int64_t> &watermarkInfo,
                      bool hasSoftFailure,
                      flatbuffers::FlatBufferBuilder &fbb);
    bool FromFBSqlResult(const isearch::fbs::SqlResult *fbsSqlResult,
                         ErrorResult &errorResult,
                         bool &hasSoftFailure,
                         std::shared_ptr<table::Table> &table);
    flatbuffers::Offset<isearch::fbs::SqlErrorResult>
    CreateSqlErrorResult(const ErrorResult &errorResult, flatbuffers::FlatBufferBuilder &fbb);
    bool FromSqlErrorResult(const isearch::fbs::SqlErrorResult *fbSqlErrorResult,
                            ErrorResult &errorResult);
    flatbuffers::Offset<isearch::fbs::TwoDimTable>
    CreateSqlTable(const std::shared_ptr<table::Table> &table, flatbuffers::FlatBufferBuilder &fbb);
    bool FromSqlTable(const isearch::fbs::TwoDimTable *fbsTable,
                      std::shared_ptr<table::Table> &table);
    std::vector<flatbuffers::Offset<isearch::fbs::TableLeaderInfo>>
    CreateLeaderInfo(const std::map<std::string, bool> &leaderInfo,
                     flatbuffers::FlatBufferBuilder &fbb);
    std::vector<flatbuffers::Offset<isearch::fbs::TableBuildWatermark>>
    CreateWatermarkInfo(const std::map<std::string, int64_t> &watermarkInfo,
                        flatbuffers::FlatBufferBuilder &fbb);
    virtual void CreateSqlTableColumnByString(
        const std::string &columnName,
        table::Column *column,
        flatbuffers::FlatBufferBuilder &fbb,
        std::vector<flatbuffers::Offset<isearch::fbs::Column>> &sqlTableColumns);
    void CreateSqlTableColumnByMultiString(
        const std::string &columnName,
        table::ColumnData<autil::MultiString> *columnData,
        flatbuffers::FlatBufferBuilder &fbb,
        std::vector<flatbuffers::Offset<isearch::fbs::Column>> &sqlTableColumns);

    void CreateSqlTableColumnByByteString( // compactiable code for java
        const std::string &columnName,
        table::Column *column,
        flatbuffers::FlatBufferBuilder &fbb,
        std::vector<flatbuffers::Offset<isearch::fbs::Column>> &sqlTableColumns);
    void CreateSqlTableColumnByMultiByteString( // compactiable code for java
        const std::string &columnName,
        table::ColumnData<autil::MultiString> *columnData,
        flatbuffers::FlatBufferBuilder &fbb,
        std::vector<flatbuffers::Offset<isearch::fbs::Column>> &sqlTableColumns);

#define DECLARE_NUMERIC_SWITCH_CASE(fbType, fieldType)                                             \
    void CreateSqlTableColumnBy##fbType(                                                           \
        const std::string &columnName,                                                             \
        table::ColumnData<matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType,               \
                                                                false>::CppType> *columnData,      \
        flatbuffers::FlatBufferBuilder &fbb,                                                       \
        std::vector<flatbuffers::Offset<isearch::fbs::Column>> &sqlTableColumns);                  \
    bool FromSqlTableColumnBy##fbType(                                                             \
        autil::mem_pool::Pool *pool,                                                               \
        const isearch::fbs::Column *fbsColumn,                                                     \
        table::ColumnData<matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType,               \
                                                                false>::CppType> *columnData);
#define DECLARE_MULTINUMERIC_SWITCH_CASE(fbType, fieldType)                                        \
    void CreateSqlTableColumnByMulti##fbType(                                                      \
        const std::string &columnName,                                                             \
        table::ColumnData<matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType,               \
                                                                true>::CppType> *columnData,       \
        flatbuffers::FlatBufferBuilder &fbb,                                                       \
        std::vector<flatbuffers::Offset<isearch::fbs::Column>> &sqlTableColumns);                  \
    bool FromSqlTableColumnByMulti##fbType(                                                        \
        autil::mem_pool::Pool *pool,                                                               \
        const isearch::fbs::Column *fbsColumn,                                                     \
        table::ColumnData<matchdoc::MatchDocBuiltinType2CppType<matchdoc::fieldType,               \
                                                                true>::CppType> *columnData);
    FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(DECLARE_NUMERIC_SWITCH_CASE)
    FLATBUFFER_NUMERIC_SWITCH_CASE_HELPER(DECLARE_MULTINUMERIC_SWITCH_CASE)
#undef DECLARE_NUMERIC_SWITCH_CASE
#undef DECLARE_MULTINUMERIC_SWITCH_CASE

    bool FromSqlTableColumnByString(autil::mem_pool::Pool *pool,
                                    const isearch::fbs::Column *fbsColumn,
                                    table::ColumnData<autil::MultiChar> *columnData);
    bool FromSqlTableColumnByByteString(autil::mem_pool::Pool *pool,
                                        const isearch::fbs::Column *fbsColumn,
                                        table::ColumnData<autil::MultiChar> *columnData);
    bool FromSqlTableColumnByMultiString(autil::mem_pool::Pool *pool,
                                         const isearch::fbs::Column *fbsColumn,
                                         table::ColumnData<autil::MultiString> *columnData);
    bool FromSqlTableColumnByMultiByteString(autil::mem_pool::Pool *pool,
                                             const isearch::fbs::Column *fbsColumn,
                                             table::ColumnData<autil::MultiString> *columnData);

private:
    bool _useByteString = false;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlResultUtil> SqlResultUtilPtr;
} // namespace sql
} // namespace isearch
