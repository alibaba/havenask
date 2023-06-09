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
#include "table/Table.h"

#include "autil/DataBuffer.h"
#include "autil/ConstString.h"
#include "matchdoc/Reference.h"
#include "matchdoc/VectorDocStorage.h"
#include "table/ValueTypeSwitch.h"

// TODO(xinfei.sxf) move to common define file
constexpr size_t NEED_COMPACT_MEM_SIZE = 4 * 1024 * 1024; // 4MB

using namespace std;
using namespace matchdoc;
using namespace autil;

namespace table {
AUTIL_LOG_SETUP(table, Table);

void TableSerializeInfo::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(*(uint32_t*)(this));
}

void TableSerializeInfo::deserialize(autil::DataBuffer &dataBuffer) {
    uint32_t seriInfo;
    dataBuffer.read(seriInfo);
    *(uint32_t*)(this) = seriInfo;
}

Table::Table(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr)
    : _allocator(new MatchDocAllocator(poolPtr))
    , _dependentPools({poolPtr})
{
}

Table::Table(const vector<MatchDoc> &matchDocs, const MatchDocAllocatorPtr &allocator, uint8_t level)
    : _rows(matchDocs)
    , _allocator(allocator)
    , _dependentPools({allocator->getPoolPtr()})
{
    _serializeInfo.serializeLevel = level;
    init();
}

Table::~Table() {
    for (auto col : _columnVec) {
        if (col) {
            delete col;
        }
    }
    _columnVec.clear();
}

void Table::init() {
    ReferenceVector refs = _allocator->getRefBySerializeLevel(
            _serializeInfo.serializeLevel);
    const ReferenceVector &subRefs = _allocator->getAllSubNeedSerializeReferences(
                    _serializeInfo.serializeLevel);
    refs.insert(refs.end(), subRefs.begin(), subRefs.end());
    for (ReferenceBase* ref : refs) {
        const string &colName = ref->getName();
        ColumnDataBase* colData = createColumnData(ref, &_rows);
        if (!colData) {
            AUTIL_LOG(INFO, "create column data failed, column name: [%s].", colName.c_str());
            continue;
        }
        ColumnSchemaPtr columnSchema(new ColumnSchema(colName, ref->getValueType()));
        _schema.addColumnSchema(columnSchema);
        Column* column = new Column(columnSchema.get(), colData);
        _columnMap.insert(pair<string, Column*>(colName, column));
        _columnVec.emplace_back(column);
    }
}

void Table::deleteRows() {
    if (_deleteFlag.empty()) {
        return;
    }
    if (_deleteFlag.size() < getRowCount()) {
        _deleteFlag.resize(getRowCount());
    }
    vector<Row> newRows;
    for (size_t i = 0; i < _rows.size(); ++i) {
        if (_deleteFlag[i]) {
            _deleteFlag[i] = false;
        } else {
            newRows.emplace_back(_rows[i]);
        }
    }
    std::swap(_rows, newRows);
}

void Table::compact() {
    if (_allocator->getDocSize() > 0 &&
        _allocator->getAllocatedCount() >
        NEED_COMPACT_MEM_SIZE / _allocator->getDocSize() + getRowCount())
    {
        _allocator->compact(_rows);
    }
}

void Table::clearRows() {
    clearFrontRows(_rows.size());
}

void Table::clearFrontRows(size_t clearSize) {
    for (size_t i = 0; i < clearSize; i++) {
        markDeleteRow(i);
    }
    deleteRows();
    compact();
}

void Table::clearBackRows(size_t clearSize) {
    size_t start = 0;
    size_t rowCount = getRowCount();
    if (rowCount >= clearSize) {
        start = rowCount - clearSize;
    }
    for (size_t i = start; i < rowCount; i++) {
        markDeleteRow(i);
    }
    deleteRows();
    compact();
}

Column* Table::declareColumn(const std::string &name, ValueType vt, bool endGroup) {
    if (vt.isStdType() || !vt.isBuiltInType() ||
        matchdoc::bt_unknown == vt.getBuiltinType())
    {
        return nullptr;
    }
    bool isMulti = vt.isMultiValue();
    matchdoc::BuiltinType bt = vt.getBuiltinType();
    return declareColumn(name, bt, isMulti, endGroup);
}

Column* Table::declareColumn(const std::string &name, matchdoc::BuiltinType bt,
                               bool isMulti, bool endGroup)
{
    auto iter = _columnMap.find(name);
    if (iter != _columnMap.end()) {
        if (iter->second != nullptr) {
            return iter->second;
        } else {
            return nullptr;
        }
    }
    Column *column = nullptr;
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        matchdoc::Reference<T>* ref = _allocator->declareWithConstructFlagDefaultGroup<T>(
                name, matchdoc::ConstructTypeTraits<T>::NeedConstruct(), SL_ATTRIBUTE);
        if (!ref) {
            AUTIL_LOG(WARN, "can not declare column [%s]", name.c_str());
            return false;
        }
        ColumnData<T>* colData = new ColumnData<T>(ref, &_rows);
        ColumnSchemaPtr columnSchema(new ColumnSchema(name, ref->getValueType()));
        _schema.addColumnSchema(columnSchema);
        column = new Column(columnSchema.get(), colData);
        return true;
    };
    if (!ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
        AUTIL_LOG(ERROR, "declare column [%s] failed", name.c_str());
        return nullptr;
    }
    if (endGroup) {
        _allocator->extend();
    }
    _columnMap.insert(std::pair<std::string, Column*>(name, column));
    _columnVec.emplace_back(column);
    return column;
}

Column* Table::declareColumnWithConstructFlag(const std::string &name, ValueType vt, bool endGroup) {
    if (vt.isStdType() || !vt.isBuiltInType() ||
        matchdoc::bt_unknown == vt.getBuiltinType())
    {
        return nullptr;
    }
    bool isMulti = vt.isMultiValue();
    matchdoc::BuiltinType bt = vt.getBuiltinType();
    return declareColumnWithConstructFlag(name, bt, isMulti, vt.needConstruct(), endGroup);
}

Column* Table::declareColumnWithConstructFlag(const std::string &name, matchdoc::BuiltinType bt,
                             bool isMulti, bool needConstruct, bool endGroup)
{
    auto iter = _columnMap.find(name);
    if (iter != _columnMap.end()) {
        if (iter->second != nullptr) {
            return iter->second;
        } else {
            return nullptr;
        }
    }
    Column *column = nullptr;
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        matchdoc::Reference<T>* ref = _allocator->declareWithConstructFlagDefaultGroup<T>(
                name, needConstruct, SL_ATTRIBUTE);
        if (!ref) {
            AUTIL_LOG(WARN, "can not declare column [%s]", name.c_str());
            return false;
        }
        ColumnData<T>* colData = new ColumnData<T>(ref, &_rows);
        ColumnSchemaPtr columnSchema(new ColumnSchema(name, ref->getValueType()));
        _schema.addColumnSchema(columnSchema);
        column = new Column(columnSchema.get(), colData);
        return true;
    };
    if (!ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
        AUTIL_LOG(ERROR, "declare column [%s] failed", name.c_str());
        return nullptr;
    }
    if (endGroup) {
        _allocator->extend();
    }
    _columnMap.insert(std::pair<std::string, Column*>(name, column));
    _columnVec.emplace_back(column);
    return column;
}

ColumnDataBase* Table::createColumnData(ReferenceBase* refBase, vector<Row>* rows)
{
    ColumnDataBase *ret = nullptr;
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        Reference<T> *ref = dynamic_cast<Reference<T> *>(refBase);
        if (unlikely(!ref)) {
            AUTIL_LOG(ERROR, "impossible cast reference failed");
            return false;
        }
        ret = new ColumnData<T>(ref, rows);
        return true;
    };
    if (!ValueTypeSwitch::switchType(refBase->getValueType(), func, func)) {
        AUTIL_LOG(ERROR, "create column data [%s] failed", refBase->getName().c_str());
        return nullptr;
    }
    return ret;
}

Column* Table::getColumn(const string &name) const {
    auto iter = _columnMap.find(name);
    if (iter == _columnMap.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

Column* Table::getColumn(size_t col) const {
    return col < getColumnCount() ? _columnVec[col] : nullptr;
}

Row Table::allocateRow() {
    Row row = _allocator->allocate();
    _rows.push_back(row);
    return row;
}
size_t Table::getRowSize() {
    uint32_t docSize = 0;
    if (_allocator) {
        docSize += _allocator->getDocSize();
        docSize += _allocator->getSubDocSize();
    }
    return docSize;
}

void Table::batchAllocateRow(size_t count) {
    const auto &rows = _allocator->batchAllocate(count);
    _rows.insert(_rows.end(), rows.begin(), rows.end());
}

bool Table::isSameSchema(const TablePtr &other) {
    if (!isSameSchema(other.get())) {
        AUTIL_LOG(WARN, "table schema is not same [%s] vs [%s]",
                explainSchema().c_str(), other->explainSchema().c_str());
        return false;
    }
    return true;
}

bool Table::isSameSchema(const Table *other) {
    if (_columnVec.size() != other->_columnVec.size()) {
        AUTIL_LOG(WARN, "column size not match [%ld] vs [%ld]",
                _columnVec.size(), other->_columnVec.size());
        return false;
    }
    for (size_t i = 0; i < _columnVec.size(); ++i) {
        auto thisCol = _columnVec[i]->getColumnSchema();
        auto otherCol = other->_columnVec[i]->getColumnSchema();
        if (*thisCol != *otherCol) {
            AUTIL_LOG(WARN, "column schema[%ld] not match [%s] vs [%s]",
                i, thisCol->toString().c_str(), otherCol->toString().c_str());
            return false;
        }
    }
    return true;
}

bool Table::merge(const TablePtr &other) {
    if (!isSameSchema(other)) {
        return false;
    }
    mergeDependentPools(other);
    if (_allocator->mergeAllocatorOnlySame(other->_allocator.get(), other->_rows, _rows)) {
        return true;
    } else {
        AUTIL_LOG(TRACE1, "copy table.");
        return copyTable(other);
    }
}

void Table::mergeDependentPools(const TablePtr &other) {
    mergeDependentPools(*other);
}

void Table::mergeDependentPools(const Table &other) {
    for (const auto &dependentPool : other.getDependentPools()) {
        _dependentPools.insert(dependentPool);
    }
}

void Table::dependPool(const autil::mem_pool::PoolPtr &pool) {
    _dependentPools.insert(pool);
}

const std::set<std::shared_ptr<autil::mem_pool::Pool>> &Table::getDependentPools() const {
    return _dependentPools;
}

bool Table::hasMultiValueColumn() const {
    return _schema.hasMultiValueColumn();
}

std::string Table::explainSchema() const {
    ostringstream oss;
    for (auto p : _columnVec) {
        oss << p->getColumnSchema()->toString() << ';';
    }
    return oss.str();
}

std::string Table::getDependentPoolListDebugString() const {
    std::ostringstream oss;
    for (const auto &dependentPool : _dependentPools) {
        const void * address = static_cast<const void*>(dependentPool.get());
        oss << address << ";";
    }
    return oss.str();
}

bool Table::copyTable(const TablePtr &other) {
    return copyTable(other.get());
}

bool Table::copyTable(Table *other) {
    if (this->getMatchDocAllocator()->hasSubDocAllocator()) {
        AUTIL_LOG(ERROR, "left table has sub doc allocator, not supported");
        return false;
    }
    if (other->getMatchDocAllocator()->hasSubDocAllocator()) {
        AUTIL_LOG(ERROR, "right table has sub doc allocator, not supported");
        return false;
    }
    if (!isSameSchema(other)) {
        return false;
    }
    size_t copyCount = other->getRowCount();
    size_t nowCount = getRowCount();
    batchAllocateRow(copyCount);
    assert (copyCount + nowCount == getRowCount());
    for (size_t i = 0; i < getColumnCount(); i++) {
        auto left = getColumn(i);
        auto right = other->getColumn(i);
        if (left == NULL || right == NULL) {
            AUTIL_LOG(ERROR, "unexpected get column failed");
            return false;
        }
        auto schema = left->getColumnSchema();
        if (schema == nullptr) {
            AUTIL_LOG(ERROR, "unexpected get column schema failed");
            return false;
        }
        auto vt = schema->getType();
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            auto columnData = left->getColumnData<T>();
            auto orignData = right->getColumnData<T>();
            if (columnData == nullptr || orignData == nullptr) {
                return false;
            }
            for (size_t k = 0; k < copyCount; k++) {
                columnData->set(this->getRow(nowCount + k), orignData->get(other->getRow(k)));
            }
            return true;
        };
        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            AUTIL_LOG(ERROR, "declare column [%s] failed", schema->toString().c_str());
            return false;
        }
    }
    return true;
}

void Table::serialize(autil::DataBuffer &dataBuffer, autil::CompressType type) const {
    bool needCompress = type != autil::CompressType::INVALID_COMPRESS_TYPE &&
                        type != autil::CompressType::NO_COMPRESS &&
                        (uint32_t)type < TableSerializeInfo::MAX_COMPRESS_VALUE;
    if (needCompress) {
        _serializeInfo.compress = (uint32_t)type;
    }
    dataBuffer.write(_serializeInfo);
    _allocator->setSortRefFlag(false);
    if (!needCompress) {
        _allocator->serialize(dataBuffer, _rows, _serializeInfo.serializeLevel);
    } else {
        autil::DataBuffer bodyBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, dataBuffer.getPool());
        _allocator->serialize(bodyBuffer, _rows, _serializeInfo.serializeLevel);
        StringView input(bodyBuffer.getData(), bodyBuffer.getDataLen());
        std::string compressedResult;
        if (!autil::CompressionUtil::compress(input, type, compressedResult, bodyBuffer.getPool())) {
            AUTIL_LOG(ERROR, "compress failed, fallback to no compression");
        }
        dataBuffer.write((uint32_t)compressedResult.size());
        dataBuffer.writeBytes(compressedResult.c_str(), compressedResult.size());
    }
}

void Table::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_serializeInfo);
    _allocator->setSortRefFlag(false);

    autil::CompressType type = autil::CompressType::NO_COMPRESS;
    if ((autil::CompressType)_serializeInfo.compress < autil::CompressType::MAX) {
        type = (autil::CompressType)_serializeInfo.compress;
    }

    bool needDecompress = type != autil::CompressType::NO_COMPRESS &&
                          type != autil::CompressType::INVALID_COMPRESS_TYPE;
    if (!needDecompress) {
        _allocator->deserialize(dataBuffer, _rows);
    } else {
        uint32_t len = 0;
        dataBuffer.read(len);
        const void *data = dataBuffer.readNoCopy(len);
        std::string decompressed;
        autil::CompressionUtil::decompress(StringView((const char*)data, len),
                type, decompressed, dataBuffer.getPool());
        autil::DataBuffer bodyBuffer((void *)decompressed.c_str(),
                decompressed.size(), dataBuffer.getPool());
        _allocator->deserialize(bodyBuffer, _rows);
    }
    init();
}

void Table::serializeToString(std::string &data, autil::mem_pool::Pool *pool,
                              autil::CompressType type) const
{
    size_t bufferSize = std::max(estimateUsedMemory(), (size_t)DataBuffer::DEFAUTL_DATA_BUFFER_SIZE);
    DataBuffer dataBuffer(bufferSize, pool);
    serialize(dataBuffer, type);
    data.append(dataBuffer.getData(), dataBuffer.getDataLen());
}

void Table::deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool) {
    deserializeFromString(data.c_str(), data.size(), pool);
}

void Table::deserializeFromString(const char *data, size_t len, autil::mem_pool::Pool *pool) {
    DataBuffer dataBuffer((void*)data, len, pool);
    deserialize(dataBuffer);
}

string Table::toString(size_t row, size_t col) const {
    assert(row < getRowCount());
    assert(col < getColumnCount());
    return _columnVec[col]->toString(row);
}


size_t Table::estimateUsedMemory() const {
    return getRowCount() * _allocator->getDocSize();
}

std::shared_ptr<Table> Table::clone(const autil::mem_pool::PoolPtr &pool) {
    TablePtr table = std::make_shared<Table>(pool);
    auto colCount = getColumnCount();
    for (size_t i = 0; i < colCount; ++i) {
        const auto& valueType = _columnVec[i]->getColumnSchema()->getType();
        const auto& name = _columnVec[i]->getColumnSchema()->getName();
        table->declareColumnWithConstructFlag(name, valueType, false);
    }
    table->endGroup();
    table->mergeDependentPools(*this);
    return table->copyTable(this) ? table : nullptr;
}

}
