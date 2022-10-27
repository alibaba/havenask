#include <ha3/sql/ops/join/LeftJoin.h>
#include <autil/TimeUtility.h>

using namespace autil;
using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, LeftJoin);

LeftJoin::LeftJoin(const JoinBaseParam &joinBaseParam)
    : JoinBase(joinBaseParam)
{
}

bool LeftJoin::generateResultTable(const std::vector<size_t> &leftTableIndexes,
                                   const std::vector<size_t> &rightTableIndexes,
                                   const TablePtr &leftTable, const TablePtr &rightTable,
                                   size_t joinedRowCount, TablePtr &outputTable)
{
    uint64_t beginOutput = TimeUtility::currentTime();
    _joinedFlag.resize(joinedRowCount);
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

    if (_calcTable != nullptr &&
        !_calcTable->filterTable(outputTable, joinIndexStart, outputTable->getRowCount(), true))
    {
        SQL_LOG(ERROR, "filter table failed");
        return false;
    }

    for (size_t i = joinIndexStart, idx = 0; i < outputTable->getRowCount(); ++i, ++idx) {
        if (!outputTable->isDeletedRow(i)) {
            _joinedFlag[leftTableIndexes[idx]] = true;
        }
    }

    uint64_t afterEvaluate = TimeUtility::currentTime();
    JoinInfoCollector::incEvaluateTime(_joinInfo, afterEvaluate - afterInitTable);

    return true;
}

bool LeftJoin::fillNotJoinedRows(const TablePtr &leftTable, size_t offset, TablePtr &outputTable)
{
    assert(_joinedFlag.size() >= offset);
    size_t joinIndexStart = outputTable->getRowCount();
    vector<size_t> leftTableIndexes;
    for (size_t index = 0; index < offset; index++) {
        if (!_joinedFlag[index]) {
            leftTableIndexes.emplace_back(index);
        }
    }
    outputTable->batchAllocateRow(leftTableIndexes.size());
    if (!evaluateColumns(_leftFields, leftTableIndexes, leftTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for left table failed");
        return false;
    }
    if (!evaluateEmptyColumns(_rightFields, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate empty columns for right table failed");
        return false;
    }
    _joinedFlag.erase(_joinedFlag.begin(), _joinedFlag.begin() + offset);
    return true;
}

bool LeftJoin::finish(TablePtr &leftTable, size_t offset, TablePtr &outputTable) {
    assert(outputTable);
    uint64_t beforeEvaluate = TimeUtility::currentTime();
    if (!fillNotJoinedRows(leftTable, offset, outputTable)) {
        SQL_LOG(ERROR, "left full table finish fill not join rows failed");
        return false;
    }
    uint64_t afterEvaluate = TimeUtility::currentTime();
    JoinInfoCollector::incEvaluateTime(_joinInfo, afterEvaluate - beforeEvaluate);
    return true;
}

END_HA3_NAMESPACE(sql);
