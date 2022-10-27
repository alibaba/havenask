#include <ha3/sql/data/Table.h>
#include <ha3/sql/common/common.h>
#include <matchdoc/Reference.h>
#include <matchdoc/CommonDefine.h>
#include <ha3/sql/util/ValueTypeSwitch.h>

using namespace std;
using namespace matchdoc;
using namespace autil;
BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, Table);
void TableSerializeInfo::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(*(uint32_t*)(this));
}

void TableSerializeInfo::deserialize(autil::DataBuffer &dataBuffer) {
    uint32_t seriInfo;
    dataBuffer.read(seriInfo);
    *(uint32_t*)(this) = seriInfo;
}

Table::Table(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr) {
    _allocator.reset(new MatchDocAllocator(poolPtr));
}

Table::Table(const vector<MatchDoc> &matchDocs, const MatchDocAllocatorPtr &allocator, uint8_t level)
    : _rows(matchDocs)
    , _allocator(allocator)
{
    _serializeInfo.serializeLevel = level;
    init();
}

void Table::init() {
    ReferenceVector refs = _allocator->getRefBySerializeLevel(
            _serializeInfo.serializeLevel);
    ReferenceVector subRefs = _allocator->getAllSubNeedSerializeReferences(_serializeInfo.serializeLevel);
    refs.insert(refs.end(), subRefs.begin(), subRefs.end());
    for (ReferenceBase* ref : refs) {
        string colName = ref->getName();
        ColumnDataBase* colData = createColumnData(ref, &_rows);
        if (!colData) {
            SQL_LOG(INFO, "create column data failed, column name: [%s].", colName.c_str());
            continue;
        }
        ColumnSchemaPtr columnSchema(new ColumnSchema(colName, ref->getValueType()));
        _schema.addColumnSchema(columnSchema);
        ColumnPtr column(new Column(columnSchema.get(), colData));
        _columnMap.insert(pair<string, ColumnPtr>(colName, column));
        _columnVec.push_back(column);
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

ColumnPtr Table::declareColumn(const std::string &name, ValueType vt, bool endGroup) {
    if (vt.isStdType() || !vt.isBuiltInType() ||
        matchdoc::bt_unknown == vt.getBuiltinType())
    {
        return ColumnPtr();
    }
    bool isMulti = vt.isMultiValue();
    matchdoc::BuiltinType bt = vt.getBuiltinType();
    return declareColumn(name, bt, isMulti, endGroup);
}

ColumnPtr Table::declareColumn(const std::string &name, matchdoc::BuiltinType bt,
                               bool isMulti, bool endGroup)
{
    auto iter = _columnMap.find(name);
    if (iter != _columnMap.end()) {
        if (iter->second != nullptr) {
            return iter->second;
        } else {
            return ColumnPtr();
        }
    }
    ColumnPtr column;
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        matchdoc::Reference<T>* ref = _allocator->declareWithConstructFlagDefaultGroup<T>(
                name, matchdoc::ConstructTypeTraits<T>::NeedConstruct(), SL_ATTRIBUTE);
        if (!ref) {
            SQL_LOG(WARN, "can not declare column [%s]", name.c_str());
            return false;
        }
        ColumnData<T>* colData = new ColumnData<T>(ref, &_rows);
        ColumnSchemaPtr columnSchema(new ColumnSchema(name, ref->getValueType()));
        _schema.addColumnSchema(columnSchema);
        column.reset(new Column(columnSchema.get(), colData));
        return true;
    };
    if (!ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
        SQL_LOG(ERROR, "declare column [%s] failed", name.c_str());
        return ColumnPtr();
    }
    if (endGroup) {
        _allocator->extend();
    }
    _columnMap.insert(std::pair<std::string, ColumnPtr>(name, column));
    _columnVec.push_back(column);
    return column;
}


ColumnDataBase* Table::createColumnData(ReferenceBase* refBase, vector<Row>* rows)
{
    ColumnDataBase *ret = nullptr;
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        Reference<T> *ref = dynamic_cast<Reference<T> *>(refBase);
        if (unlikely(!ref)) {
            SQL_LOG(ERROR, "impossible cast reference failed");
            return false;
        }
        ret = new ColumnData<T>(ref, rows);
        return true;
    };
    if (!ValueTypeSwitch::switchType(refBase->getValueType(), func, func)) {
        SQL_LOG(ERROR, "create column data [%s] failed", refBase->getName().c_str());
        return nullptr;
    }
    return ret;
}

Table::~Table() {
}

ColumnPtr Table::getColumn(const string &name) const {
    auto iter = _columnMap.find(name);
    if (iter == _columnMap.end()) {
        return ColumnPtr();
    } else {
        return iter->second;
    }
}

ColumnPtr Table::getColumn(size_t col) const {
    return col < getColumnCount() ? _columnVec[col] : ColumnPtr();
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
    if (_columnVec.size() != other->_columnVec.size()) {
        return false;
    }
    for (size_t i = 0; i < _columnVec.size(); ++i) {
        if ((*_columnVec[i]->getColumnSchema()) !=
            (*other->_columnVec[i]->getColumnSchema()))
        {
            return false;
        }
    }
    return true;
}

bool Table::merge(const TablePtr &other) {
    if (!isSameSchema(other)) {
        SQL_LOG(WARN, "table schema is not same [%s] vs [%s]",
                _schema.toString().c_str(), other->getTableSchema().toString().c_str());
        return false;
    }
    if (_allocator->mergeAllocatorOnlySame(other->_allocator.get(), other->_rows, _rows)) {
        return true;
    } else {
        SQL_LOG(TRACE1, "copy table.");
        return copyTable(other);
    }
}

bool Table::copyTable(const TablePtr &other) {
    if (this->getMatchDocAllocator()->hasSubDocAllocator()) {
        SQL_LOG(ERROR, "left table has sub doc allocator, not supported");
        return false;
    }
    if (other->getMatchDocAllocator()->hasSubDocAllocator()) {
        SQL_LOG(ERROR, "right table has sub doc allocator, not supported");
        return false;
    }
    if (!isSameSchema(other)) {
        SQL_LOG(WARN, "table schema is not same [%s] vs [%s]",
                        _schema.toString().c_str(), other->getTableSchema().toString().c_str());
        return false;
    }
    size_t copyCount = other->getRowCount();
    size_t nowCount = getRowCount();
    batchAllocateRow(copyCount);
    assert (copyCount + nowCount == getRowCount());
    for (size_t i = 0; i < getColumnCount(); i++) {
        ColumnPtr left = getColumn(i);
        ColumnPtr right = other->getColumn(i);
        if (left == NULL || right == NULL) {
            SQL_LOG(ERROR, "unexpected get column failed");
            return false;
        }
        auto schema = left->getColumnSchema();
        if (schema == nullptr) {
            SQL_LOG(ERROR, "unexpected get column schema failed");
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
            SQL_LOG(ERROR, "declare column [%s] failed", schema->toString().c_str());
            return false;
        }
    }
    return true;
}

void Table::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_serializeInfo);
    _allocator->setSortRefFlag(false);
    _allocator->serialize(dataBuffer, _rows, _serializeInfo.serializeLevel);
}

void Table::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_serializeInfo);
    _allocator->setSortRefFlag(false);
    _allocator->deserialize(dataBuffer, _rows);
    init();
}

void Table::serializeToString(std::string &data, autil::mem_pool::Pool *pool) const {
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool);
    serialize(dataBuffer);
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


END_HA3_NAMESPACE(sql);
