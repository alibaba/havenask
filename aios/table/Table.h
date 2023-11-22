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
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/CompressionUtil.h"
#include "autil/result/Result.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/TableSchema.h"

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace table {

class Table;
typedef std::shared_ptr<Table> TablePtr;

class Table {
public:
    static constexpr uint32_t COMPACT_THRESHOLD_IN_BYTES = 4 * 1024 * 1024;

public:
    Table();
    Table(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr);
    ~Table();

private:
    Table(const Table &);
    Table &operator=(const Table &);

public:
    inline size_t getRowCount() const { return _rows.size(); }
    inline size_t getColumnCount() const { return _columnVec.size(); }
    Column *getColumn(const std::string &name) const;
    Column *getColumn(size_t col) const;

    template <typename T>
    Column *declareColumn(const std::string &name);
    Column *declareColumn(const std::string &name, matchdoc::BuiltinType bt, bool isMulti);
    Column *declareColumn(const std::string &name, ValueType valType);

    bool addColumn(std::shared_ptr<Column> column);

    const TableSchema &getTableSchema() const { return _schema; }

    autil::mem_pool::Pool *getPool() { return _pool.get(); }

    /**
     * @brief Make a new table from given column-name projection. The underlying data buffers of
     *        new table is shared from the original one.
     *
     * @note It is your own duty to prevent shared data from being "written".
     * @param projection    The column-name projection (new, old).
     * @param pool          Specify a memory pool for the new table, or create it automatically.
     * @return autil::Result<TablePtr>
     */
    autil::Result<TablePtr> project(const std::vector<std::pair<std::string, std::string>> &projection,
                                    const std::shared_ptr<autil::mem_pool::Pool> &pool = nullptr);

    // ..
    bool merge(const TablePtr &other);
    bool copyTable(Table *other);
    Row getRow(size_t rowIndex) const {
        assert(rowIndex < _rows.size());
        return _rows[rowIndex];
    }
    Row allocateRow();
    void batchAllocateRow(size_t count);
    void resize(uint32_t size);
    inline void markDeleteRow(size_t rowIndex) {
        assert(rowIndex < getRowCount());
        _rows[rowIndex].setDeleted();
    }
    bool isDeletedRow(size_t rowIndex) const {
        assert(rowIndex < getRowCount());
        return _rows[rowIndex].isDeleted();
    }
    std::vector<Row> getRows() { return _rows; }
    void deleteRows();
    bool compact(uint32_t bytesThreshold = COMPACT_THRESHOLD_IN_BYTES);
    void clearRows();
    void clearFrontRows(size_t clearSize);
    void clearBackRows(size_t clearSize);

    void setRows(std::vector<Row> rows) { _rows = std::move(rows); }

    TablePtr clone(const autil::mem_pool::PoolPtr &pool);

    uint32_t usedBytes() const;

public:
    void serialize(autil::DataBuffer &dataBuffer, autil::CompressType type = autil::CompressType::NO_COMPRESS) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    void serializeToString(std::string &data,
                           autil::mem_pool::Pool *pool,
                           autil::CompressType type = autil::CompressType::NO_COMPRESS) const;
    void deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool);
    void deserializeFromString(const char *data, size_t len, autil::mem_pool::Pool *pool);

private:
    template <typename T>
    Column *createColumn(const std::string &name);

    void serializeImpl(autil::DataBuffer &dataBuffer) const;
    void deserializeImpl(autil::DataBuffer &dataBuffer);

    void swap(Table &other);

public:
    static std::unique_ptr<Table> fromMatchDocs(const std::vector<matchdoc::MatchDoc> &matchDocs,
                                                const matchdoc::MatchDocAllocatorPtr &allocator,
                                                uint8_t level = SL_ATTRIBUTE,
                                                const std::vector<std::string> &extraColumns = {});
    static matchdoc::MatchDocAllocatorPtr toMatchDocs(const Table &table);

private:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::vector<std::shared_ptr<Column>> _columnVec;
    std::unordered_map<std::string, size_t> _columnIndex;
    uint32_t _size = 0;
    uint32_t _capacity = 0;
    std::vector<Row> _rows;
    TableSchema _schema;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
Column *Table::declareColumn(const std::string &name) {
    ValueType vt = matchdoc::ValueTypeHelper<T>::getValueType();
    return declareColumn(name, vt);
}

} // namespace table
