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
#include "ha3/sql/ops/agg/AggBase.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/agg/Aggregator.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/ValueTypeSwitch.h"
#include "table/TableUtil.h"

namespace isearch {
namespace sql {
class AggFuncManager;
} // namespace sql
} // namespace isearch

namespace table {
template <typename T>
class ColumnData;
} // namespace table

using namespace std;
using namespace table;
using namespace autil;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AggBase);

bool AggBase::init(const AggFuncManager *aggFuncManager,
                   const std::vector<std::string> &groupKeyVec,
                   const std::vector<std::string> &outputFields,
                   const std::vector<AggFuncDesc> &aggFuncDesc) {
    SQL_LOG(DEBUG,
            "init aggbase with groupKeyVec [%s] outputFields [%s] aggFuncDesc [%s]",
            StringUtil::toString(groupKeyVec).c_str(),
            StringUtil::toString(outputFields).c_str(),
            ToJsonString(aggFuncDesc).c_str());
    _aggFuncManager = aggFuncManager;
    _groupKeyVec = groupKeyVec;
    _outputFields = outputFields;
    _aggFuncDesc = aggFuncDesc;
    return doInit();
}

bool AggBase::computeAggregator(TablePtr &input,
                                const std::vector<AggFuncDesc> &aggFuncDesc,
                                Aggregator &aggregator,
                                bool &aggregatorReady) {
    if (!aggregatorReady) {
        if (!aggregator.init(_aggFuncManager, aggFuncDesc, _groupKeyVec, _outputFields, input)) {
            SQL_LOG(ERROR, "aggregator init failed");
            return false;
        }
        aggregatorReady = true;
    }
    vector<size_t> groupKeys;
    if (!_groupKeyVec.empty()) {
        if (!TableUtil::calculateGroupKeyHash(input, _groupKeyVec, groupKeys)) {
            SQL_LOG(ERROR, "calculate group key hash failed");
            return false;
        }
    } else {
        groupKeys.resize(input->getRowCount(), 0);
    }
    if (!aggregator.aggregate(input, groupKeys)) {
        SQL_LOG(ERROR, "aggregate failed");
        return false;
    }
    return true;
}


} // namespace sql
} // namespace isearch
