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

class AggLocal : public AggBase {
public:
    AggLocal(navi::GraphMemoryPoolR *graphMemoryPoolR, AggHints aggHints = AggHints())
        : _aggregatorReady(false)
        , _localAggregator(AggFuncMode::AGG_FUNC_MODE_LOCAL, graphMemoryPoolR, aggHints) {}

private:
    AggLocal(const AggLocal &);
    AggLocal &operator=(const AggLocal &);

public:
    bool compute(table::TablePtr &input) override {
        return computeAggregator(input, _aggFuncDesc, _localAggregator, _aggregatorReady);
    }
    table::TablePtr getTable() override {
        return _localAggregator.getTable();
    }

    void getStatistics(uint64_t &collectTime,
                       uint64_t &outputAccTime,
                       uint64_t &mergeTime,
                       uint64_t &outputResultTime,
                       uint64_t &aggPoolSize) const override {
        _localAggregator.getStatistics(collectTime, outputAccTime, aggPoolSize);
        mergeTime = 0;
        outputResultTime = 0;
    }

private:
    bool _aggregatorReady;
    Aggregator _localAggregator;
};

typedef std::shared_ptr<AggLocal> AggLocalPtr;
} // namespace sql
