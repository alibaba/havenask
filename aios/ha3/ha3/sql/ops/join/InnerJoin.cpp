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
#include "ha3/sql/ops/join/InnerJoin.h"

#include <stdint.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/join/JoinBase.h"
#include "ha3/sql/ops/join/JoinInfoCollector.h"

using namespace autil;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, InnerJoin);

InnerJoin::InnerJoin(const JoinBaseParam &joinBaseParam)
    : JoinBase(joinBaseParam) {}

bool InnerJoin::generateResultTable(const std::vector<size_t> &leftTableIndexes,
                                    const std::vector<size_t> &rightTableIndexes,
                                    const table::TablePtr &leftTable,
                                    const table::TablePtr &rightTable,
                                    size_t joinedRowCount,
                                    table::TablePtr &outputTable) {
    uint64_t beginOutput = TimeUtility::currentTime();
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
    uint64_t afterEvaluate = TimeUtility::currentTime();
    if (_joinBaseParam._calcTable != nullptr
        && !_joinBaseParam._calcTable->filterTable(outputTable, joinIndexStart,
                outputTable->getRowCount(), true))
    {
        SQL_LOG(ERROR, "filter table failed");
        return false;
    }
    JoinInfoCollector::incEvaluateTime(_joinBaseParam._joinInfo,
            afterEvaluate - afterInitTable);
    return true;
}

} // namespace sql
} // namespace isearch
