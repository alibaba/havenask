
#ifndef ISEARCH_TABLE_H
#define ISEARCH_TABLE_H

#include <ha3/sql/data/Column.h>
#include <ha3/sql/data/TableSchema.h>
#include <ha3/sql/data/Row.h>
#include <navi/engine/Data.h>
#include <matchdoc/MatchDocAllocator.h>

BEGIN_HA3_NAMESPACE(sql);

class MultiColumnComparator;
class TableUtil;

struct TableSerializeInfo {
    TableSerializeInfo()
        : version(0)
        , compress(0)
        , serializeLevel(SL_ATTRIBUTE)
    {}
    uint32_t version:4;
    uint32_t compress:4;
    uint32_t serializeLevel:8;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
};

class Table : public navi::Data
{
public:
    Table(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr);
    Table(const std::vector<matchdoc::MatchDoc> &matchDocs,
          const matchdoc::MatchDocAllocatorPtr &allocator,
          uint8_t level = SL_ATTRIBUTE);
    ~Table();
private:
    Table(const Table &);
    Table& operator=(const Table &);
public:
    inline size_t getRowCount() const {
        return _rows.size();
    }
    inline size_t getColumnCount() const {
        return _columnVec.size();
    }
    ColumnPtr getColumn(const std::string &name) const;
    ColumnPtr getColumn(size_t col) const;

    matchdoc::ValueType getColumnType(size_t col) const {
        assert(col < getColumnCount());
        return _columnVec[col]->getColumnSchema()->getType();
    }

    const std::string &getColumnName(size_t col) const {
        assert(col < getColumnCount());
        return _columnVec[col]->getColumnSchema()->getName();
    }
    template <typename T>
    ColumnPtr declareColumn(const std::string &name, bool endGroup = true);

    ColumnPtr declareColumn(const std::string &name, matchdoc::BuiltinType bt,
                            bool isMulti, bool endGroup = true);
    ColumnPtr declareColumn(const std::string &name, ValueType valType,
                            bool endGroup = true);

    void endGroup() {
        if (_allocator) {
            _allocator->extend();
        }
    }
    const TableSchema& getTableSchema() const {
        return _schema;
    }
    bool isSameSchema(const std::shared_ptr<Table> &table);

    bool merge(const std::shared_ptr<Table> &other);
    bool copyTable(const std::shared_ptr<Table> &other);
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
            _deleteFlag.resize(getRowCount());
        }
        _deleteFlag[rowIndex] = true;
    }
    bool isDeletedRow(size_t rowIndex) {
        assert(rowIndex < getRowCount());
        if (_deleteFlag.size() < getRowCount()) {
            _deleteFlag.resize(getRowCount());
        }
        return _deleteFlag[rowIndex];
    }
    const std::vector<Row>& getRows() { return _rows; }
    void deleteRows();
    void compact();
    void clearRows();
    void clearFrontRows(size_t clearSize);
    void clearBackRows(size_t clearSize);
    matchdoc::MatchDocAllocator *getMatchDocAllocator() { // to friend for calc
        return _allocator.get();
    }
    void appendRows(const std::vector<Row> &rows) {
        _rows.insert(_rows.end(), rows.begin(), rows.end());
    }
    void setRows(std::vector<Row> &rows) {
        _rows = std::move(rows);
        _deleteFlag.clear();
    }

public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    void serializeToString(std::string &data, autil::mem_pool::Pool *pool) const;
    void deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool);
    void deserializeFromString(const char *data, size_t len, autil::mem_pool::Pool *pool);
protected:
    std::string toString(size_t row, size_t col) const;
    void setColumnVec(std::vector<ColumnPtr> &columnVec) {
        assert(columnVec.size() == getColumnCount());
        _columnVec = std::move(columnVec);
    }
    friend class TableUtil;
private:
    void init();
    ColumnDataBase* createColumnData(matchdoc::ReferenceBase* refBase,
                                     std::vector<Row>* rows);
private:
    std::vector<ColumnPtr> _columnVec;
    std::map<std::string, ColumnPtr> _columnMap;
    std::vector<bool> _deleteFlag; // for rows
    std::vector<Row> _rows;
    matchdoc::MatchDocAllocatorPtr _allocator;
    TableSchema _schema;
    TableSerializeInfo _serializeInfo;
private:
    HA3_LOG_DECLARE();
};

template <typename T>
ColumnPtr Table::declareColumn(const std::string &name, bool endGroup) {
    ValueType vt = matchdoc::ValueTypeHelper<T>::getValueType();
    if (vt.isStdType() || !vt.isBuiltInType() || matchdoc::bt_unknown == vt.getBuiltinType()) {
        return ColumnPtr();
    }
    auto iter = _columnMap.find(name);
    if (iter != _columnMap.end()) {
        if (iter->second->getColumnData<T>() != nullptr) {
            return iter->second;
        } else {
            return ColumnPtr();
        }
    }
    matchdoc::Reference<T>* ref = _allocator->declareWithConstructFlagDefaultGroup<T>(name, matchdoc::ConstructTypeTraits<T>::NeedConstruct(), SL_ATTRIBUTE);
    if (!ref) {
        return ColumnPtr();
    }
    if (endGroup) {
        _allocator->extend();
    }
    ColumnData<T>* colData = new ColumnData<T>(ref, &_rows);
    ColumnSchemaPtr columnSchema(new ColumnSchema(name, ref->getValueType()));
    _schema.addColumnSchema(columnSchema);
    ColumnPtr column(new Column(columnSchema.get(), colData));
    _columnMap.insert(std::pair<std::string, ColumnPtr>(name, column));
    _columnVec.push_back(column);
    return column;
}

typedef std::shared_ptr<Table> TablePtr;

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_TABLE_H
