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
#pragma once

#include <memory>
#include <stdint.h>

#include "autil/Log.h"
#include "sql/ops/agg/AggBase.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "sql/ops/agg/Aggregator.h"
#include "table/Table.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace navi {
class GraphMemoryPoolR;
} // namespace navi

namespace sql {

class AggGlobal : public AggBase {
public:
    AggGlobal(navi::GraphMemoryPoolR *graphMemoryPoolR, AggHints aggHints = AggHints())
        : _aggregatorReady(false)
        , _globalAggregator(AggFuncMode::AGG_FUNC_MODE_GLOBAL, graphMemoryPoolR, aggHints) {}

private:
    AggGlobal(const AggGlobal &);
    AggGlobal &operator=(const AggGlobal &);

public:
    bool compute(table::TablePtr &input) override {
        return computeAggregator(input, _aggFuncDesc, _globalAggregator, _aggregatorReady);
    }
    table::TablePtr getTable() override {
        return _globalAggregator.getTable();
    }
    void getStatistics(uint64_t &collectTime,
                       uint64_t &outputAccTime,
                       uint64_t &mergeTime,
                       uint64_t &outputResultTime,
                       uint64_t &aggPoolSize) const override {
        collectTime = 0;
        outputAccTime = 0;
        _globalAggregator.getStatistics(mergeTime, outputResultTime, aggPoolSize);
    }

private:
    bool _aggregatorReady;
    Aggregator _globalAggregator;
};

typedef std::shared_ptr<AggGlobal> AggGlobalPtr;
} // namespace sql
