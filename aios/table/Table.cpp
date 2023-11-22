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
#include "autil/result/Errors.h"
#include "matchdoc/Reference.h"
#include "table/ValueTypeSwitch.h"

using namespace matchdoc;
using namespace autil;
using namespace autil::result;

namespace table {
AUTIL_LOG_SETUP(table, Table);

struct TableSerializeInfo {
    TableSerializeInfo() : version(0), compress(0) {}
    uint32_t version  : 4;
    uint32_t compress : 4;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    static constexpr uint32_t MAX_COMPRESS_VALUE = 15; // 4 bits
};

void TableSerializeInfo::serialize(autil::DataBuffer &dataBuffer) const { dataBuffer.write(*(uint32_t *)(this)); }

void TableSerializeInfo::deserialize(autil::DataBuffer &dataBuffer) {
    uint32_t seriInfo;
    dataBuffer.read(seriInfo);
    *(uint32_t *)(this) = seriInfo;
}

Table::Table() : Table(std::make_shared<autil::mem_pool::Pool>()) {}

Table::Table(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr) : _pool(poolPtr) {}

Table::~Table() = default;

void Table::deleteRows() {
    size_t j = 0;
    for (size_t i = 0; i < _rows.size(); ++i) {
        if (_rows[i].isDeleted()) {
            continue;
        } else {
            if (i > j) {
                _rows[j] = _rows[i];
            }
            ++j;
        }
    }
    if (j < _rows.size()) {
        _rows.erase(_rows.begin() + j, _rows.end());
    }
}

bool Table::compact(uint32_t bytesThreshold) {
    if (_schema.hasUserTypeColumn()) {
        AUTIL_LOG(INFO, "do not support compact for user type column");
        return false;
    }

    uint32_t bytes = usedBytes();
    uint32_t estimatedWasteBytes = bytes * _rows.size() * 1.0 / _size;
    if (estimatedWasteBytes < bytesThreshold) {
        return false;
    }

    AUTIL_LOG(INFO,
              "compact table, allocated row count: %u, in use row count: %u, bytes: %u, waste bytes: %u",
              _size,
              (uint32_t)_rows.size(),
              bytes,
              estimatedWasteBytes);
    auto newTable = clone(_pool);
    if (!newTable) {
        AUTIL_LOG(ERROR, "clone table failed");
        return false;
    }
    swap(*newTable);
    return true;
}

void Table::swap(Table &other) {
    _columnVec.swap(other._columnVec);
    _columnIndex.swap(other._columnIndex);
    std::swap(_size, other._size);
    std::swap(_capacity, other._capacity);
    _rows.swap(other._rows);
    _schema.swap(other._schema);
    for (auto &col : _columnVec) {
        col->attachRows(&_rows);
    }
}

uint32_t Table::usedBytes() const {
    uint32_t bytes = 0;
    for (auto &col : _columnVec) {
        bytes += col->usedBytes(_size);
    }
    return bytes;
}

void Table::clearRows() { clearFrontRows(_rows.size()); }

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

Column *Table::declareColumn(const std::string &name, ValueType vt) {
    if (vt.isStdType() || !vt.isBuiltInType() || matchdoc::bt_unknown == vt.getBuiltinType()) {
        return nullptr;
    }
    bool isMulti = vt.isMultiValue();
    matchdoc::BuiltinType bt = vt.getBuiltinType();
    return declareColumn(name, bt, isMulti);
}

Column *Table::declareColumn(const std::string &name, matchdoc::BuiltinType bt, bool isMulti) {
    Column *column = getColumn(name);
    if (column != nullptr) {
        return column;
    }
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        column = createColumn<T>(name);
        return column != nullptr;
    };
    if (!ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
        AUTIL_LOG(ERROR, "declare column [%s] failed", name.c_str());
        return nullptr;
    }

    return column;
}

Column *Table::getColumn(const std::string &name) const {
    auto it = _columnIndex.find(name);
    if (it == _columnIndex.end()) {
        return nullptr;
    }
    return getColumn(it->second);
}

Column *Table::getColumn(size_t col) const { return col < getColumnCount() ? _columnVec[col].get() : nullptr; }

Row Table::allocateRow() {
    batchAllocateRow(1);
    return _rows.back();
}

void Table::batchAllocateRow(size_t count) {
    if (_size + count > _capacity) {
        resize(_size + count);
    }
    for (size_t i = 0; i < count; ++i) {
        _rows.emplace_back(Row(_size++));
    }
}

void Table::resize(uint32_t size) {
    if (size <= _capacity) {
        // TODO: support shrink
        return;
    }
    size = matchdoc::VectorStorage::getAlignSize(size);
    for (auto &col : _columnVec) {
        col->resize(size);
    }
    _capacity = size;
}

Result<TablePtr> Table::project(const std::vector<std::pair<std::string, std::string>> &projection,
                                const std::shared_ptr<autil::mem_pool::Pool> &pool) {
    auto table = std::make_shared<Table>(pool ? pool : std::make_shared<autil::mem_pool::Pool>());
    table->_size = _size;
    table->_capacity = _capacity;
    table->_rows = _rows;
    for (auto &[name1, name] : projection) {
        auto col = getColumn(name);
        AR_REQUIRE_TRUE(col, RuntimeError::make("column [%s] not exist", name.c_str()));
        auto data = col->getBaseColumnData()->share(&table->_rows);
        AR_REQUIRE_TRUE(data, RuntimeError::make("share column [%s] failed", name.c_str()));
        auto col1 = std::make_shared<Column>(name1, col->getType(), std::move(data));
        auto ok = table->addColumn(col1);
        AR_REQUIRE_TRUE(ok, RuntimeError::make("add column [%s] failed", col1->getColumnSchema()->toString().c_str()));
    }
    return table;
}

bool Table::merge(const TablePtr &other) {
    if (other->_rows.empty()) {
        return true;
    }
    if (_schema != other->_schema) {
        AUTIL_LOG(ERROR,
                  "schema not equal, left: %s, right: %s",
                  _schema.toString().c_str(),
                  other->_schema.toString().c_str());
        return false;
    }
    bool succ = true;
    for (size_t i = 0; i < _columnVec.size(); ++i) {
        Column *thisCol = _columnVec[i].get();
        Column *otherCol = other->getColumn(thisCol->getName());
        if (!thisCol->merge(*otherCol)) {
            succ = false;
            break;
        }
    }
    if (succ) {
        for (const auto &row : other->_rows) {
            Row r(_capacity + row.getIndex());
            if (row.isDeleted()) {
                r.setDeleted();
            }
            _rows.emplace_back(std::move(r));
        }
        _size = _capacity + other->_size;
        _capacity += other->_capacity;
        return true;
    } else {
        return copyTable(other.get());
    }
}

bool Table::copyTable(Table *other) {
    if (_schema != other->_schema) {
        AUTIL_LOG(ERROR,
                  "schema not equal, left: %s, right: %s",
                  _schema.toString().c_str(),
                  other->_schema.toString().c_str());
        return false;
    }

    size_t copyCount = other->getRowCount();
    size_t nowCount = getRowCount();
    batchAllocateRow(copyCount);
    assert(copyCount + nowCount == getRowCount());
    for (size_t i = 0; i < getColumnCount(); i++) {
        auto left = getColumn(i);
        auto right = other->getColumn(i);
        if (left == nullptr || right == nullptr) {
            AUTIL_LOG(ERROR, "unexpected get column failed");
            return false;
        }
        if (!left->copy(nowCount, *right, 0, copyCount)) {
            AUTIL_LOG(ERROR, "copy column %s failed", left->getName().c_str());
            return false;
        }
    }
    return true;
}

void Table::serialize(autil::DataBuffer &dataBuffer, autil::CompressType type) const {
    bool needCompress = type != autil::CompressType::INVALID_COMPRESS_TYPE &&
                        type != autil::CompressType::NO_COMPRESS &&
                        (uint32_t)type < TableSerializeInfo::MAX_COMPRESS_VALUE;
    TableSerializeInfo serializeInfo;
    if (needCompress) {
        serializeInfo.compress = (uint32_t)type;
    }
    dataBuffer.write(serializeInfo);
    if (!needCompress) {
        serializeImpl(dataBuffer);
    } else {
        autil::DataBuffer bodyBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, dataBuffer.getPool());
        serializeImpl(bodyBuffer);
        StringView input(bodyBuffer.getData(), bodyBuffer.getDataLen());
        std::string compressedResult;
        if (!autil::CompressionUtil::compress(input, type, compressedResult, bodyBuffer.getPool())) {
            AUTIL_LOG(ERROR, "compress failed, fallback to no compression");
        }
        dataBuffer.write((uint32_t)compressedResult.size());
        dataBuffer.writeBytes(compressedResult.c_str(), compressedResult.size());
    }
}

void Table::serializeImpl(autil::DataBuffer &dataBuffer) const {
    // schema
    uint32_t count = _columnVec.size();
    dataBuffer.write(count);
    for (auto &col : _columnVec) {
        const auto *schema = col->getColumnSchema();
        assert(schema != nullptr);
        dataBuffer.write(*schema);
    }

    // rows
    size_t rowCount = _rows.size();
    dataBuffer.write(rowCount);
    for (size_t i = 0; i < rowCount; ++i) {
        dataBuffer.write(_rows[i].getDocId());
    }

    // columns
    for (auto &col : _columnVec) {
        assert(col != nullptr);
        col->serializeData(dataBuffer, _rows);
    }
}

void Table::deserialize(autil::DataBuffer &dataBuffer) {
    TableSerializeInfo serializeInfo;
    dataBuffer.read(serializeInfo);

    autil::CompressType type = autil::CompressType::NO_COMPRESS;
    if ((autil::CompressType)serializeInfo.compress < autil::CompressType::MAX) {
        type = (autil::CompressType)serializeInfo.compress;
    }

    bool needDecompress =
        type != autil::CompressType::NO_COMPRESS && type != autil::CompressType::INVALID_COMPRESS_TYPE;
    if (!needDecompress) {
        deserializeImpl(dataBuffer);
    } else {
        uint32_t len = 0;
        dataBuffer.read(len);
        const void *data = dataBuffer.readNoCopy(len);
        std::string decompressed;
        autil::CompressionUtil::decompress(
            StringView((const char *)data, len), type, decompressed, dataBuffer.getPool());
        autil::DataBuffer bodyBuffer((void *)decompressed.c_str(), decompressed.size(), dataBuffer.getPool());
        deserializeImpl(bodyBuffer);
    }
}

void Table::deserializeImpl(autil::DataBuffer &dataBuffer) {
    _columnVec.clear();
    _columnIndex.clear();
    // schema
    uint32_t columnCount = 0;
    dataBuffer.read(columnCount);
    for (uint32_t i = 0; i < columnCount; ++i) {
        ColumnSchema schema;
        dataBuffer.read(schema);
        Column *column = declareColumn(schema.getName(), schema.getType());
        if (column == nullptr) {
            return;
        }
    }

    // rows
    size_t rowCount = 0;
    dataBuffer.read(rowCount);
    _rows.clear();
    batchAllocateRow(rowCount);
    for (size_t i = 0; i < rowCount; ++i) {
        int32_t docId = -1;
        dataBuffer.read(docId);
        _rows[i].setDocId(docId);
    }

    // data
    for (auto &col : _columnVec) {
        col->deserializeData(dataBuffer, _rows);
    }
}

void Table::serializeToString(std::string &data, autil::mem_pool::Pool *pool, autil::CompressType type) const {
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool);
    serialize(dataBuffer, type);
    data.append(dataBuffer.getData(), dataBuffer.getDataLen());
}

void Table::deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool) {
    deserializeFromString(data.c_str(), data.size(), pool);
}

void Table::deserializeFromString(const char *data, size_t len, autil::mem_pool::Pool *pool) {
    DataBuffer dataBuffer((void *)data, len, pool);
    deserialize(dataBuffer);
}

std::shared_ptr<Table> Table::clone(const autil::mem_pool::PoolPtr &pool) {
    TablePtr table = std::make_shared<Table>(pool);
    auto colCount = getColumnCount();
    for (size_t i = 0; i < colCount; ++i) {
        const auto &valueType = _columnVec[i]->getColumnSchema()->getType();
        const auto &name = _columnVec[i]->getColumnSchema()->getName();
        table->declareColumn(name, valueType);
    }
    return table->copyTable(this) ? table : nullptr;
}

template <typename T>
Column *Table::createColumn(const std::string &name) {
    auto columnData = std::make_unique<ColumnData<T>>(_pool);
    auto column = std::make_shared<Column>(name, matchdoc::ValueTypeHelper<T>::getValueType(), std::move(columnData));
    Column *result = column.get();
    column->resize(_capacity);
    if (!addColumn(std::move(column))) {
        return nullptr;
    }
    return result;
}

bool Table::addColumn(std::shared_ptr<Column> column) {
    const auto &name = column->getName();
    if (_columnIndex.count(name) > 0) {
        return false;
    }
    _schema.addColumn(column->getSchemaPtr());
    column->attachRows(&_rows);
    _columnIndex.emplace(name, _columnVec.size());
    _columnVec.emplace_back(std::move(column));
    return true;
}

std::unique_ptr<Table> Table::fromMatchDocs(const std::vector<matchdoc::MatchDoc> &matchDocs,
                                            const matchdoc::MatchDocAllocatorPtr &allocator,
                                            uint8_t level,
                                            const std::vector<std::string> &extraColumns) {
    allocator->extend();
    auto table = std::make_unique<Table>();
    table->_pool = allocator->getPoolPtr();
    table->_size = allocator->getAllocatedCount();
    table->_capacity = allocator->getCapacity();
    table->_rows = matchDocs;
    auto refs = allocator->getRefBySerializeLevel(level);
    auto subRefs = allocator->getAllSubNeedSerializeReferences(level);
    if (!subRefs.empty()) {
        refs.insert(refs.end(), subRefs.begin(), subRefs.end());
    }
    for (const auto &name : extraColumns) {
        auto ref = allocator->findReferenceWithoutType(name);
        if (ref != nullptr) {
            refs.push_back(ref);
        }
    }
    for (auto ref : refs) {
        std::unique_ptr<Column> column;
        bool needCopy = ref->isMount() || ref->isSubDocReference();
        auto type = ref->getValueType();
        if (!type.isBuiltInType()) {
            if (needCopy) {
                AUTIL_LOG(ERROR, "do not support convert %s to Column", ref->toDebugString().c_str());
                return nullptr;
            } else {
                if (ref->needConstructMatchDoc()) {
                    for (uint32_t i = allocator->getAllocatedCount(); i < allocator->getCapacity(); ++i) {
                        ref->construct(matchdoc::MatchDoc(i));
                    }
                }
                column = Column::makeUserTypeColumnFromReference(ref);
                // hack: MatchDocAllocator::~MatchDocAllocator() will force destruct all docs
                ref->setNeedDestructMatchDoc(false);
            }
        } else {
            auto fromRefImpl = [&](auto tag) {
                using T = typename decltype(tag)::value_type;
                auto typedRef = dynamic_cast<matchdoc::Reference<T> *>(ref);
                if (typedRef == nullptr) {
                    return false;
                }
                if (!needCopy) {
                    column = Column::fromReference<T>(typedRef);
                } else {
                    column = Column::make<T>(typedRef->getName(), table->_pool);
                    column->resize(table->_capacity);
                    auto columnData = column->getColumnData<T>();
                    for (const auto &doc : matchDocs) {
                        columnData->set(doc, typedRef->get(doc));
                    }
                }
                return column != nullptr;
            };
            if (!ValueTypeSwitch::switchType(ref->getValueType(), fromRefImpl, fromRefImpl)) {
                return nullptr;
            }
        }
        if (!table->addColumn(std::move(column))) {
            AUTIL_LOG(ERROR, "add column %s to table failed", ref->getName().c_str());
            return nullptr;
        }
    }
    return table;
}

matchdoc::MatchDocAllocatorPtr Table::toMatchDocs(const Table &table) {
    std::vector<std::unique_ptr<matchdoc::FieldGroup>> fieldGroups;
    for (auto &col : table._columnVec) {
        auto ref = col->toReference();
        auto fieldGroup = matchdoc::FieldGroup::makeSingleFieldGroup(ref.release());
        fieldGroups.emplace_back(std::move(fieldGroup));
    }
    return matchdoc::MatchDocAllocator::fromFieldGroups(
        table._pool, std::move(fieldGroups), table._size, table._capacity);
}

} // namespace table
