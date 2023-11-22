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
#include "sql/ops/join/InnerJoin.h"

#include <memory>
#include <stdint.h>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "sql/common/Log.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/join/JoinBase.h"

using namespace autil;

namespace sql {

InnerJoin::InnerJoin(const JoinBaseParamR &joinBaseParam)
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

    size_t joinIndexStart = outputTable->getRowCount();
    uint64_t afterInitTable = TimeUtility::currentTime();
    _joinBaseParam._joinInfoR->incInitTableTime(afterInitTable - beginOutput);
    if (!evaluateJoinedTable(
            leftTableIndexes, rightTableIndexes, leftTable, rightTable, outputTable)) {
        SQL_LOG(ERROR, "evaluate join table failed");
        return false;
    }
    uint64_t afterEvaluate = TimeUtility::currentTime();
    if (_joinBaseParam._calcTableR != nullptr
        && !_joinBaseParam._calcTableR->filterTable(
            outputTable, joinIndexStart, outputTable->getRowCount(), true)) {
        SQL_LOG(ERROR, "filter table failed");
        return false;
    }
    _joinBaseParam._joinInfoR->incEvaluateTime(afterEvaluate - afterInitTable);
    return true;
}

} // namespace sql
