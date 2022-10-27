#include <ha3/sql/ops/join/SemiJoin.h>
#include <autil/TimeUtility.h>

using namespace autil;
using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, SemiJoin);

SemiJoin::SemiJoin(const JoinBaseParam &joinBaseParam)
    : JoinBase(joinBaseParam)
{
}

bool SemiJoin::generateResultTable(const std::vector<size_t> &leftTableIndexes,
                                   const std::vector<size_t> &rightTableIndexes,
                                   const TablePtr &leftTable, const TablePtr &rightTable,
                                   size_t joinedRowCount, TablePtr &outputTable)
{
    uint64_t beginOutput = TimeUtility::currentTime();
    _joinedFlag.resize(joinedRowCount);
    if (_joinedTable == nullptr) {
        _joinedTable.reset(new Table(_poolPtr));
        if (!JoinBase::initJoinedTable(leftTable, rightTable, _joinedTable)) {
            SQL_LOG(ERROR, "init join table failed");
            return false;
        }
    } else {
        _joinedTable->clearRows();
    }
    uint64_t afterInitTable = TimeUtility::currentTime();
    JoinInfoCollector::incInitTableTime(_joinInfo, afterInitTable - beginOutput);
    if (!evaluateJoinedTable(leftTableIndexes, rightTableIndexes,
                             leftTable, rightTable, _joinedTable))
    {
        SQL_LOG(ERROR, "evaluate join table failed");
        return false;
    }

    if (_calcTable != nullptr &&
        !_calcTable->filterTable(_joinedTable, 0, _joinedTable->getRowCount(), true))
    {
        SQL_LOG(ERROR, "filter table failed");
        return false;
    }

    vector<size_t> tableIndexes;
    for (size_t i = 0, idx = 0; i < _joinedTable->getRowCount(); ++i, ++idx) {
        if (!_joinedTable->isDeletedRow(i)) {
            if (!_joinedFlag[leftTableIndexes[idx]]) {
                tableIndexes.emplace_back(leftTableIndexes[idx]);
                _joinedFlag[leftTableIndexes[idx]] = true;
            }
        }
    }

    if (outputTable == nullptr) {
        outputTable.reset(new Table(_poolPtr));
        if (!initJoinedTable(leftTable, rightTable, outputTable)) {
            return false;
        }
    }

    JoinInfoCollector::incJoinCount(_joinInfo, tableIndexes.size());
    size_t joinIndexStart = outputTable->getRowCount();
    outputTable->batchAllocateRow(tableIndexes.size());
    if (!evaluateColumns(_leftFields, tableIndexes, leftTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for left table failed");
        return false;
    }

    uint64_t afterEvaluate = TimeUtility::currentTime();
    JoinInfoCollector::incEvaluateTime(_joinInfo, afterEvaluate - afterInitTable);

    return true;
}

bool SemiJoin::finish(TablePtr &leftTable, size_t offset, TablePtr &outputTable) {
    _joinedFlag.erase(_joinedFlag.begin(), _joinedFlag.begin() + offset);
    return true;
}

bool SemiJoin::initJoinedTable(const TablePtr &leftTable, const TablePtr &rightTable,
                               TablePtr &outputTable)
{
    if (!appendColumns(_leftFields, leftTable, outputTable)) {
        SQL_LOG(ERROR, "append columns for left table failed");
        return false;
    }
    outputTable->endGroup();
    return true;
}


END_HA3_NAMESPACE(sql);
