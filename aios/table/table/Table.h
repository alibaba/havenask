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
namespace matchdoc {
class MatchDoc;
class ReferenceBase;
template <typename T>
class Reference;
} // namespace matchdoc

namespace table {

struct TableSerializeInfo {
    TableSerializeInfo() : version(0), compress(0), serializeLevel(SL_ATTRIBUTE) {}
    uint32_t version        : 4;
    uint32_t compress       : 4;
    uint32_t serializeLevel : 8;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    static constexpr uint32_t MAX_COMPRESS_VALUE = 31; // 4 bits
};

class Table {
public:
    Table(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr);
    Table(const std::vector<matchdoc::MatchDoc> &matchDocs,
          const matchdoc::MatchDocAllocatorPtr &allocator,
          uint8_t level = SL_ATTRIBUTE);
    ~Table();

private:
    Table(const Table &);
    Table &operator=(const Table &);

public:
    inline size_t getRowCount() const { return _rows.size(); }
    inline size_t getColumnCount() const { return _columnVec.size(); }
    Column *getColumn(const std::string &name) const;
    Column *getColumn(size_t col) const;

    matchdoc::ValueType getColumnType(size_t col) const {
        assert(col < getColumnCount());
        return _columnVec[col]->getColumnSchema()->getType();
    }

    const std::string &getColumnName(size_t col) const {
        assert(col < getColumnCount());
        return _columnVec[col]->getColumnSchema()->getName();
    }
    template <typename T>
    Column *declareColumn(const std::string &name, bool endGroup = true);
    Column *declareColumn(const std::string &name, matchdoc::BuiltinType bt, bool isMulti, bool endGroup = true);
    Column *declareColumn(const std::string &name, ValueType valType, bool endGroup = true);
    void endGroup() {
        if (_allocator) {
            _allocator->extend();
        }
    }
    const TableSchema &getTableSchema() const { return _schema; }

    autil::mem_pool::Pool *getDataPool() { return _allocator->getSessionPool(); }

    bool merge(const std::shared_ptr<Table> &other);
    bool copyTable(Table *other);
    Row getRow(size_t rowIndex) const {
        assert(rowIndex < _rows.size());
        return _rows[rowIndex];
    }
    size_t getRowSize();
    Row allocateRow();
    void batchAllocateRow(size_t count);
    inline void markDeleteRow(size_t rowIndex) {
        assert(rowIndex < getRowCount());
        if (unlikely(_deleteFlag.size() < getRowCount())) {
            _deleteFlag.resize(getRowCount(), false);
        }
        _deleteFlag[rowIndex] = true;
    }
    bool isDeletedRow(size_t rowIndex) const {
        assert(rowIndex < getRowCount());
        if (rowIndex < _deleteFlag.size()) {
            return _deleteFlag[rowIndex];
        }
        return false;
    }
    std::vector<Row> getRows() { return _rows; }
    void deleteRows();
    void compact();
    void clearRows();
    void clearFrontRows(size_t clearSize);
    void clearBackRows(size_t clearSize);
    matchdoc::MatchDocAllocator *getMatchDocAllocator() { // to friend for calc
        return _allocator.get();
    }
    // TODO: ?????
    matchdoc::MatchDocAllocatorPtr getMatchDocAllocatorPtr() { // to friend for calc
        return _allocator;
    }
    void appendRows(const std::vector<Row> &rows) { _rows.insert(_rows.end(), rows.begin(), rows.end()); }
    void setRows(std::vector<Row> rows) {
        _rows = std::move(rows);
        _deleteFlag.clear();
    }

    std::shared_ptr<Table> clone(const autil::mem_pool::PoolPtr &pool);

    void mergeDependentPools(const std::shared_ptr<table::Table> &other);
    void mergeDependentPools(const Table &other);
    void dependPool(const autil::mem_pool::PoolPtr &pool);

    const std::set<std::shared_ptr<autil::mem_pool::Pool>> &getDependentPools() const;

public:
    void serialize(autil::DataBuffer &dataBuffer, autil::CompressType type = autil::CompressType::NO_COMPRESS) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    void serializeToString(std::string &data,
                           autil::mem_pool::Pool *pool,
                           autil::CompressType type = autil::CompressType::NO_COMPRESS) const;
    void deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool);
    void deserializeFromString(const char *data, size_t len, autil::mem_pool::Pool *pool);

protected:
    std::string toString(size_t row, size_t col) const;
    friend class TableUtil;

private:
    void init();
    ColumnDataBase *createColumnData(matchdoc::ReferenceBase *refBase, std::vector<Row> *rows);
    Column *declareColumnWithConstructFlag(
        const std::string &name, matchdoc::BuiltinType bt, bool isMulti, bool needConstruct, bool endGroup = true);
    Column *declareColumnWithConstructFlag(const std::string &name, ValueType vt, bool endGroup);
    size_t estimateUsedMemory() const;

private:
    std::vector<Column *> _columnVec;
    std::unordered_map<std::string, Column *> _columnMap;
    std::vector<bool> _deleteFlag; // for rows
    std::vector<Row> _rows;
    matchdoc::MatchDocAllocatorPtr _allocator;
    TableSchema _schema;
    mutable TableSerializeInfo _serializeInfo;
    std::set<std::shared_ptr<autil::mem_pool::Pool>> _dependentPools;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
Column *Table::declareColumn(const std::string &name, bool endGroup) {
    ValueType vt = matchdoc::ValueTypeHelper<T>::getValueType();
    if (!vt.isBuiltInType() || matchdoc::bt_unknown == vt.getBuiltinType()) {
        return nullptr;
    }
    auto iter = _columnMap.find(name);
    if (iter != _columnMap.end()) {
        if (iter->second->getColumnData<T>() != nullptr) {
            return iter->second;
        } else {
            return nullptr;
        }
    }
    matchdoc::Reference<T> *ref = _allocator->declareWithConstructFlagDefaultGroup<T>(
        name, matchdoc::ConstructTypeTraits<T>::NeedConstruct(), SL_ATTRIBUTE);
    if (!ref) {
        return nullptr;
    }
    if (endGroup) {
        _allocator->extend();
    }
    ColumnData<T> *colData = new ColumnData<T>(ref, &_rows);
    ColumnSchemaPtr columnSchema(new ColumnSchema(name, ref->getValueType()));
    _schema.addColumnSchema(columnSchema);
    Column *column = new Column(columnSchema.get(), colData);
    _columnMap.insert(std::pair<std::string, Column *>(name, column));
    _columnVec.push_back(column);
    return column;
}

typedef std::shared_ptr<Table> TablePtr;

} // namespace table
