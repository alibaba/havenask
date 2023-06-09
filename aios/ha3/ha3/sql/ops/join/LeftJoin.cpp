/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/sql/ops/join/LeftJoin.h"

#include <assert.h>
#include <stdint.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/join/JoinBase.h"
#include "ha3/sql/ops/join/JoinInfoCollector.h"
#include "table/Row.h"

using namespace std;
using namespace autil;
using namespace table;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, LeftJoin);

LeftJoin::LeftJoin(const JoinBaseParam &joinBaseParam)
    : JoinBase(joinBaseParam) {}

bool LeftJoin::generateResultTable(const std::vector<size_t> &leftTableIndexes,
                                   const std::vector<size_t> &rightTableIndexes,
                                   const table::TablePtr &leftTable,
                                   const table::TablePtr &rightTable,
                                   size_t joinedRowCount,
                                   table::TablePtr &outputTable) {
    uint64_t beginOutput = TimeUtility::currentTime();
    _joinedFlag.resize(joinedRowCount);
    if (outputTable == nullptr) {
        outputTable.reset(new table::Table(_joinBaseParam._poolPtr));
        if (!initJoinedTable(leftTable, rightTable, outputTable)) {
            SQL_LOG(ERROR, "init join table failed");
            return false;
        }
    }
    outputTable->mergeDependentPools(leftTable);
    outputTable->mergeDependentPools(rightTable);
    size_t joinIndexStart = outputTable->getRowCount();
    uint64_t afterInitTable = TimeUtility::currentTime();
    JoinInfoCollector::incInitTableTime(_joinBaseParam._joinInfo,
            afterInitTable - beginOutput);
    if (!evaluateJoinedTable(leftTableIndexes, rightTableIndexes, leftTable, rightTable,
                             outputTable))
    {
        SQL_LOG(ERROR, "evaluate join table failed");
        return false;
    }

    if (_joinBaseParam._calcTable != nullptr
        && !_joinBaseParam._calcTable->filterTable(outputTable, joinIndexStart,
                outputTable->getRowCount(), true))
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
    JoinInfoCollector::incEvaluateTime(_joinBaseParam._joinInfo,
            afterEvaluate - afterInitTable);

    return true;
}

bool LeftJoin::fillNotJoinedRows(const table::TablePtr &leftTable,
                                 size_t offset,
                                 table::TablePtr &outputTable) {
    assert(_joinedFlag.size() >= offset);
    size_t joinIndexStart = outputTable->getRowCount();
    vector<size_t> leftTableIndexes;
    for (size_t index = 0; index < offset; index++) {
        if (!_joinedFlag[index]) {
            leftTableIndexes.emplace_back(index);
        }
    }
    outputTable->batchAllocateRow(leftTableIndexes.size());
    if (!evaluateLeftTableColumns(leftTableIndexes, leftTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for left table failed");
        return false;
    }
    size_t rightFieldIndex = _joinBaseParam._leftInputFields.size();
    if (!evaluateEmptyColumns(rightFieldIndex, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate empty columns for right table failed");
        return false;
    }
    _joinedFlag.erase(_joinedFlag.begin(), _joinedFlag.begin() + offset);
    return true;
}

bool LeftJoin::finish(const table::TablePtr &leftTable, size_t offset, table::TablePtr &outputTable) {
    assert(outputTable);
    uint64_t beforeEvaluate = TimeUtility::currentTime();
    if (!fillNotJoinedRows(leftTable, offset, outputTable)) {
        SQL_LOG(ERROR, "left full table finish fill not join rows failed");
        return false;
    }
    uint64_t afterEvaluate = TimeUtility::currentTime();
    JoinInfoCollector::incEvaluateTime(_joinBaseParam._joinInfo,
            afterEvaluate - beforeEvaluate);
    return true;
}

} // namespace sql
} // namespace isearch
