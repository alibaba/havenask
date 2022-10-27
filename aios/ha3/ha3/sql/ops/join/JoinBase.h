#pragma once

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/join/JoinInfoCollector.h>
#include <ha3/sql/ops/calc/CalcTable.h>

BEGIN_HA3_NAMESPACE(sql);

struct JoinBaseParam {
    JoinBaseParam(const std::vector<std::string> &leftInputFields,
             const std::vector<std::string> &rightInputFields,
             const std::vector<std::string> &outputFields,
             JoinInfo *joinInfo,
             std::shared_ptr<autil::mem_pool::Pool> poolPtr,
             autil::mem_pool::Pool *pool,
             CalcTablePtr calcTable)
        : _leftInputFields(leftInputFields)
        , _rightInputFields(rightInputFields)
        , _outputFields(outputFields)
        , _joinInfo(joinInfo)
        , _poolPtr(poolPtr)
        , _pool(pool)
        , _calcTable(calcTable)
    {
    }
    std::vector<std::string> _leftInputFields;
    std::vector<std::string> _rightInputFields;
    std::vector<std::string> _outputFields;
    JoinInfo *_joinInfo;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    autil::mem_pool::Pool *_pool;
    CalcTablePtr _calcTable;
};

class JoinBase {
public:
    JoinBase(const JoinBaseParam &joinBaseParam);
    ~JoinBase() {}
    JoinBase(const JoinBase&) = delete;
    JoinBase& operator=(const JoinBase &) = delete;

public:
    virtual bool generateResultTable(const std::vector<size_t> &leftTableIndexes,
            const std::vector<size_t> &rightTableIndexes,
            const TablePtr &leftTable, const TablePtr &rightTable,
            size_t joinedRowCount, TablePtr &outputTable) = 0;
    virtual bool finish(TablePtr &leftTable, size_t offset, TablePtr &outputTable) {
        return true;
    }
    virtual bool initJoinedTable(const TablePtr &leftTable, const TablePtr &rightTable,
                                 TablePtr &outputTable);

    void setDefaultValue(const std::map<std::string, std::string> &defaultValue) {
        _defaultValue = defaultValue;
    }
protected:
    bool evaluateJoinedTable(const std::vector<size_t> &leftTableIndexes,
                             const std::vector<size_t> &rightTableIndexes,
                             const TablePtr &leftTable, const TablePtr &rightTable,
                             TablePtr &outputTable);
    bool evaluateColumns(const std::vector<std::pair<std::string, std::string> > &fields,
                         const std::vector<size_t> &rowIndexes,
                         const TablePtr &inputTable,
                         size_t joinIndexStart,
                         TablePtr &outputTable);
    bool evaluateEmptyColumns(const std::vector<std::pair<std::string, std::string>> &fields,
                              size_t joinIndexStart,
                              TablePtr &outputTable);
    bool appendColumns(const std::vector<std::pair<std::string, std::string> > &fields,
                       const TablePtr &inputTable, TablePtr &outputTable) const;
private:
    void getFieldPairsFromInput(const std::vector<std::string> &leftInputFields,
                                const std::vector<std::string> &rightInputFields,
                                const std::vector<std::string> &outputFields);
    bool getColumnInfo(const TablePtr &table,
                       const std::string &field,
                       ColumnPtr &tableColumn,
                       ValueType &vt) const;
protected:
    std::vector<std::pair<std::string, std::string>> _leftFields;
    std::vector<std::pair<std::string, std::string>> _rightFields;
    JoinInfo *_joinInfo;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    autil::mem_pool::Pool *_pool;
    CalcTablePtr _calcTable;
    std::map<std::string, std::string> _defaultValue;
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(JoinBase);

END_HA3_NAMESPACE(sql);
