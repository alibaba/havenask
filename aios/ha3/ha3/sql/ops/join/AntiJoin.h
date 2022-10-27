#pragma once

#include <ha3/sql/ops/join/JoinBase.h>

BEGIN_HA3_NAMESPACE(sql);

class AntiJoin : public JoinBase {
public:
    AntiJoin(const JoinBaseParam &joinBaseParam);
    ~AntiJoin() {}
    AntiJoin(const AntiJoin&) = delete;
    AntiJoin& operator=(const AntiJoin &) = delete;

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

HA3_TYPEDEF_PTR(AntiJoin);

END_HA3_NAMESPACE(sql);
