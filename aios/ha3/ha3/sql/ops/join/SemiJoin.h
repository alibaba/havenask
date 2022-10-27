#pragma once

#include <ha3/sql/ops/join/JoinBase.h>

BEGIN_HA3_NAMESPACE(sql);

class SemiJoin : public JoinBase {
public:
    SemiJoin(const JoinBaseParam &joinBaseParam);
    ~SemiJoin() {}
    SemiJoin(const SemiJoin&) = delete;
    SemiJoin& operator=(const SemiJoin &) = delete;

public:
    bool generateResultTable(const std::vector<size_t> &leftTableIndexes,
                             const std::vector<size_t> &rightTableIndexes,
                             const TablePtr &leftTable, const TablePtr &rightTable,
                             size_t joinedRowCount, TablePtr &outputTable) override;
    bool finish(TablePtr &leftTable, size_t offset, TablePtr &outputTable) override;
    bool initJoinedTable(const TablePtr &leftTable, const TablePtr &rightTable,
                         TablePtr &outputTable) override;
private:
    std::vector<bool> _joinedFlag;
    TablePtr _joinedTable;
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SemiJoin);

END_HA3_NAMESPACE(sql);
