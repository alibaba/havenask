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
#include "sql/ops/join/SemiJoin.h"

#include <cstddef>
#include <memory>
#include <stdint.h>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "sql/common/Log.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/join/JoinInfoCollector.h"

using namespace std;
using namespace table;

namespace sql {

SemiJoin::SemiJoin(const JoinBaseParam &joinBaseParam)
    : JoinBase(joinBaseParam) {}

bool SemiJoin::generateResultTable(const std::vector<size_t> &leftTableIndexes,
                                   const std::vector<size_t> &rightTableIndexes,
                                   const table::TablePtr &leftTable,
                                   const table::TablePtr &rightTable,
                                   size_t joinedRowCount,
                                   table::TablePtr &outputTable) {
    uint64_t beginOutput = autil::TimeUtility::currentTime();
    _joinedFlag.resize(joinedRowCount);
    if (_joinedTable == nullptr) {
        _joinedTable.reset(new Table(_joinBaseParam._poolPtr));
        _joinedTable->mergeDependentPools(leftTable);
        _joinedTable->mergeDependentPools(rightTable);
        if (!JoinBase::initJoinedTable(leftTable, rightTable, _joinedTable)) {
            SQL_LOG(ERROR, "init join table failed");
            return false;
        }
    } else {
        _joinedTable->clearRows();
    }
    uint64_t afterInitTable = autil::TimeUtility::currentTime();
    JoinInfoCollector::incInitTableTime(_joinBaseParam._joinInfo, afterInitTable - beginOutput);
    if (!evaluateJoinedTable(
            leftTableIndexes, rightTableIndexes, leftTable, rightTable, _joinedTable)) {
        SQL_LOG(ERROR, "evaluate join table failed");
        return false;
    }

    if (_joinBaseParam._calcTableR != nullptr
        && !_joinBaseParam._calcTableR->filterTable(
            _joinedTable, 0, _joinedTable->getRowCount(), true)) {
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
        outputTable.reset(new Table(_joinBaseParam._poolPtr));
        outputTable->mergeDependentPools(leftTable);
        outputTable->mergeDependentPools(rightTable);
        if (!initJoinedTable(leftTable, rightTable, outputTable)) {
            return false;
        }
    }

    JoinInfoCollector::incJoinCount(_joinBaseParam._joinInfo, tableIndexes.size());
    size_t joinIndexStart = outputTable->getRowCount();
    outputTable->batchAllocateRow(tableIndexes.size());
    if (!evaluateLeftTableColumns(tableIndexes, leftTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for left table failed");
        return false;
    }

    uint64_t afterEvaluate = autil::TimeUtility::currentTime();
    JoinInfoCollector::incEvaluateTime(_joinBaseParam._joinInfo, afterEvaluate - afterInitTable);

    return true;
}

bool SemiJoin::finish(const table::TablePtr &leftTable,
                      size_t offset,
                      table::TablePtr &outputTable) {
    _joinedFlag.erase(_joinedFlag.begin(), _joinedFlag.begin() + offset);
    return true;
}

bool SemiJoin::initJoinedTable(const table::TablePtr &leftTable,
                               const table::TablePtr &rightTable,
                               table::TablePtr &outputTable) {
    if (!appendColumns(leftTable, outputTable)) {
        SQL_LOG(ERROR, "append columns for left table failed");
        return false;
    }
    return true;
}

} // namespace sql
