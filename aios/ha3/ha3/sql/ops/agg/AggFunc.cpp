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
#include "ha3/sql/ops/agg/AggFunc.h"

#include "alog/Logger.h"
#include "ha3/sql/common/Log.h"

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, AggFunc);

bool AggFunc::initMergeInput(const table::TablePtr &inputTable) {
    SQL_LOG(ERROR, "func [%s] not implemented", __FUNCTION__);
    return false;
}
bool AggFunc::initResultOutput(const table::TablePtr &outputTable) {
    SQL_LOG(ERROR, "func [%s] not implemented", __FUNCTION__);
    return false;
}
bool AggFunc::merge(table::Row inputRow, Accumulator *acc) {
    SQL_LOG(ERROR, "func [%s] not implemented", __FUNCTION__);
    return false;
}
bool AggFunc::outputResult(Accumulator *acc, table::Row outputRow) const {
    SQL_LOG(ERROR, "func [%s] not implemented", __FUNCTION__);
    return false;
}

} // namespace sql
} // namespace isearch
