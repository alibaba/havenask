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
#include "ha3/sql/ops/orcScan/RowGroupToTableConverter.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <set>
#include <utility>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/orc/TypeUtils.h"
#include "indexlib/index/orc/RowGroup.h"
#include "indexlib/index/orc/OrcColumnVectorBatchIterator.h"
#include "indexlib/index/orc/TypeTraits.h"
#include "matchdoc/MatchDocAllocator.h"
#include "orc/MemoryPool.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/Table.h"

namespace isearch {
namespace sql {

AUTIL_DECLARE_AND_SETUP_LOGGER(sql, RowGroupToTableConverter);

static bool convertStringColumn(table::Table *table,
                                const std::string &columnName,
                                orc::ColumnVectorBatch *data,
                                uint64_t startRowId)
{
    auto column = table->declareColumn<autil::MultiChar>(columnName);
    if (column == nullptr) {
        return false;
    }
    if (data == nullptr) {
        return true;
    }
    auto stringBatch = dynamic_cast<orc::StringVectorBatch *>(data);
    if (stringBatch == nullptr) {
        return false;
    }

    auto columnData = column->getColumnData<autil::MultiChar>();
    assert(columnData != nullptr);

    auto pool = table->getMatchDocAllocator()->getPool();
    for (uint64_t i = startRowId; i < stringBatch->numElements; ++i) {
        char *buf = autil::MultiValueCreator::createMultiValueBuffer<char>(
            stringBatch->data[i], stringBatch->length[i], pool);
        autil::MultiChar mc(buf);
        columnData->set(i - startRowId, mc);
    }

    return true;
}

template <orc::TypeKind kind>
bool convertColumnTyped(table::Table *table,
                        const std::string &columnName,
                        orc::ColumnVectorBatch *data,
                        uint64_t startRowId)
{
    if constexpr (kind == orc::STRING) {
        return convertStringColumn(table, columnName, data, startRowId);
    } else {
        using CppType = typename indexlibv2::index::OrcKindTypeTraits<kind>::CppType;
        using ColumnIterator = indexlibv2::index::OrcColumnVectorBatchIterator<kind>;
        auto column = table->template declareColumn<CppType>(columnName);
        if (column == nullptr) {
            return false;
        }
        if (data == nullptr) {
            return true;
        }
        ColumnIterator iterator(data);
        auto columnData = column->template getColumnData<CppType>();
        for (size_t i = startRowId; i < iterator.Size(); ++i) {
            columnData->set(i - startRowId, iterator[i]);
        }
        return true;
    }
}

static bool convertColumnPrimitive(table::Table *table,
                                   const std::string &columnName,
                                   const orc::Type *type,
                                   orc::ColumnVectorBatch *data,
                                   uint64_t startRowId) {
#define CASE(kind)                                                      \
    case kind:                                                          \
        return convertColumnTyped<kind>(table, columnName, data, startRowId); \
        break

    switch (type->getKind()) {
        CASE(orc::BYTE);
        CASE(orc::SHORT);
        CASE(orc::INT);
        CASE(orc::LONG);
        CASE(orc::FLOAT);
        CASE(orc::DOUBLE);
        CASE(orc::STRING);
    default:
        AUTIL_LOG(ERROR, "unexpected type: %s", type->toString().c_str());
        return false;
    }
#undef CASE
}

template <orc::TypeKind kind>
bool convertColumnMultiTyped(table::Table *table,
                             const std::string &columnName,
                             orc::ListVectorBatch *listData,
                             uint64_t startRowId)
{
    using CppType = typename indexlibv2::index::OrcKindTypeTraits<kind>::CppType;
    typedef autil::MultiValueType<CppType> MultiValueTyped;
    using ElementBatchType = typename indexlibv2::index::OrcKindTypeTraits<kind>::VectorBatchType;
    auto column = table->template declareColumn<MultiValueTyped>(columnName);
    if (column == nullptr) {
        return false;
    }
    if (listData == nullptr) {
        return true;
    }

    auto elements = dynamic_cast<ElementBatchType *>(listData->elements.get());
    assert(elements != nullptr);
    CppType *base = reinterpret_cast<CppType *>(elements->data.data());

    auto columnData = column->template getColumnData<MultiValueTyped>();
    auto pool = table->getMatchDocAllocator()->getPool();
    for (size_t i = startRowId; i < listData->numElements; ++i) {
         auto start = listData->offsets[i];
         auto end = listData->offsets[i + 1];
         size_t count = end - start;
         char *buf = autil::MultiValueCreator::createMultiValueBuffer<CppType>(
                 base + start, count, pool);
         MultiValueTyped mv(buf);
         columnData->set(i - startRowId, mv);
    }
    return true;
}

template <>
inline bool convertColumnMultiTyped<orc::STRING>(table::Table *table,
                                                 const std::string &columnName,
                                                 orc::ListVectorBatch *listData,
                                                 uint64_t startRowId)
{
    auto column = table->declareColumn<autil::MultiString>(columnName);
    if (column == nullptr) {
        return false;
    }
    if (listData == nullptr) {
        return true;
    }

    auto elements = dynamic_cast<orc::StringVectorBatch *>(listData->elements.get());
    if (elements == nullptr) {
        return false;
    }

    auto columnData = column->getColumnData<autil::MultiString>();
    auto pool = table->getMatchDocAllocator()->getPool();

    for (size_t i = startRowId; i < listData->numElements; ++i) {
        auto start = listData->offsets[i];
        auto end = listData->offsets[i + 1];
        std::vector<autil::StringView> strs;
        for (auto j = start; j < end; ++j) {
            strs.emplace_back(elements->data[j], elements->length[j]);
        }
        char *buf = autil::MultiValueCreator::createMultiStringBuffer(strs, pool);
        autil::MultiString ms(buf);
        columnData->set(i - startRowId, ms);
    }

    return true;
}

static bool convertColumn(table::Table *table,
                          const std::string &columnName,
                          const orc::Type *type,
                          orc::ColumnVectorBatch *data,
                          uint64_t startRowId) {
    if (type->getKind() != orc::LIST) {
        return convertColumnPrimitive(table, columnName, type, data, startRowId);
    }

    auto listVector = dynamic_cast<orc::ListVectorBatch *>(data);

#define MULTI_CASE(kind)                                                \
    case kind:                                                          \
        return convertColumnMultiTyped<kind>(table, columnName, listVector, startRowId); \
        break

    switch (type->getSubtype(0)->getKind()) {
        MULTI_CASE(orc::BYTE);
        MULTI_CASE(orc::SHORT);
        MULTI_CASE(orc::INT);
        MULTI_CASE(orc::LONG);
        MULTI_CASE(orc::FLOAT);
        MULTI_CASE(orc::DOUBLE);
        MULTI_CASE(orc::STRING);
    default:
        AUTIL_LOG(ERROR, "unexpected type: %s", type->toString().c_str());
        return false;
    }
#undef MULTI_CASE
}

bool RowGroupToTableConverter::convert(indexlibv2::index::RowGroup *rowGroup,
                                       const std::vector<std::string> &requiredCols,
                                       table::Table *table)
{
    table->batchAllocateRow(rowGroup->GetRowCount());

    for (const auto &col : requiredCols) {
        size_t idx = rowGroup->GetColumnIdx(col);
        if (idx == (size_t)-1) {
            AUTIL_LOG(ERROR, "not found column: %s", col.c_str());
            return false;
        }
        const orc::Type *type = rowGroup->GetColumnType(idx);
        if (!indexlibv2::index::TypeUtils::TypeSupported(type)) {
            AUTIL_LOG(ERROR, "unsupported orc type: %s", type->toString().c_str());
            return false;
        }
        if (!convertColumn(table, col, type, rowGroup->GetColumn(idx), 0)) {
            AUTIL_LOG(
                ERROR, "convert %s with  type %s failed", col.c_str(), type->toString().c_str());
            return false;
        }
    }

    return true;
}


} // namespace sql
} // namespace isearch
