#include <ha3/sql/ops/join/InnerJoin.h>
#include <autil/TimeUtility.h>

using namespace autil;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, InnerJoin);

InnerJoin::InnerJoin(const JoinBaseParam &joinBaseParam)
    : JoinBase(joinBaseParam)
{
}


bool InnerJoin::generateResultTable(const std::vector<size_t> &leftTableIndexes,
                                    const std::vector<size_t> &rightTableIndexes,
                                    const TablePtr &leftTable, const TablePtr &rightTable,
                                    size_t joinedRowCount, TablePtr &outputTable)
{
    uint64_t beginOutput = TimeUtility::currentTime();
    if (outputTable == nullptr) {
        outputTable.reset(new Table(_poolPtr));
        if (!initJoinedTable(leftTable, rightTable, outputTable)) {
            SQL_LOG(ERROR, "init join table failed");
            return false;
        }
    }
    size_t joinIndexStart = outputTable->getRowCount();
    uint64_t afterInitTable = TimeUtility::currentTime();
    JoinInfoCollector::incInitTableTime(_joinInfo, afterInitTable - beginOutput);
    if (!evaluateJoinedTable(leftTableIndexes, rightTableIndexes,
                             leftTable, rightTable, outputTable))
    {
        SQL_LOG(ERROR, "evaluate join table failed");
        return false;
    }
    uint64_t afterEvaluate = TimeUtility::currentTime();
    if (_calcTable != nullptr &&
        !_calcTable->filterTable(outputTable, joinIndexStart, outputTable->getRowCount(), true))
    {
        SQL_LOG(ERROR, "filter table failed");
        return false;
    }
    JoinInfoCollector::incEvaluateTime(_joinInfo, afterEvaluate - afterInitTable);
    return true;
}

END_HA3_NAMESPACE(sql);
