#pragma once

#include <ha3/sql/ops/join/JoinBase.h>

BEGIN_HA3_NAMESPACE(sql);

class LeftJoin : public JoinBase {
public:
    LeftJoin(const JoinBaseParam &joinBaseParam);
    ~LeftJoin() {}
    LeftJoin(const LeftJoin&) = delete;
    LeftJoin& operator=(const LeftJoin &) = delete;

public:
    bool generateResultTable(const std::vector<size_t> &leftTableIndexes,
                             const std::vector<size_t> &rightTableIndexes,
                             const TablePtr &leftTable, const TablePtr &rightTable,
                             size_t joinedRowCount, TablePtr &outputTable) override;
    bool finish(TablePtr &leftTable, size_t offset, TablePtr &outputTable) override;
private:
    bool fillNotJoinedRows(const TablePtr &leftTable, size_t offset, TablePtr &outputTable);
private:
    std::vector<bool> _joinedFlag;
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LeftJoin);

END_HA3_NAMESPACE(sql);
