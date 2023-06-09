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
#include "ha3/sql/ops/agg/AggNormal.h"

#include <ostream>

#include "ha3/sql/ops/agg/AggFuncMode.h"
#include "table/Row.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace table;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AggNormal);

AggNormal::AggNormal(navi::GraphMemoryPoolResource *memoryPoolResource,
                     AggHints aggHints)
    : _aggregatorReady(false)
    , _normalAggregator(AggFuncMode::AGG_FUNC_MODE_NORMAL, memoryPoolResource, aggHints) {}

bool AggNormal::compute(TablePtr &input) {
    return computeAggregator(input, _aggFuncDesc, _normalAggregator, _aggregatorReady);
}

TablePtr AggNormal::getTable() {
    return _normalAggregator.getTable();
}

void AggNormal::getStatistics(uint64_t &collectTime,
                              uint64_t &outputAccTime,
                              uint64_t &mergeTime,
                              uint64_t &outputResultTime,
                              uint64_t &aggPoolSize) const {
    _normalAggregator.getStatistics(collectTime, outputResultTime, aggPoolSize);
    outputAccTime = 0;
    mergeTime = 0;
}

} // namespace sql
} // namespace isearch
