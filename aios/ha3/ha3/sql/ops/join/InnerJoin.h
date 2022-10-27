#pragma once

#include <ha3/sql/ops/join/JoinBase.h>

BEGIN_HA3_NAMESPACE(sql);

class InnerJoin : public JoinBase {
public:
    InnerJoin(const JoinBaseParam &joinBaseParam);
    ~InnerJoin() {}
    InnerJoin(const InnerJoin&) = delete;
    InnerJoin& operator=(const InnerJoin &) = delete;

public:
    bool generateResultTable(const std::vector<size_t> &leftTableIndexes,
                             const std::vector<size_t> &rightTableIndexes,
                             const TablePtr &leftTable, const TablePtr &rightTable,
                             size_t joinedRowCount, TablePtr &outputTable) override;
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(InnerJoin);

END_HA3_NAMESPACE(sql);
