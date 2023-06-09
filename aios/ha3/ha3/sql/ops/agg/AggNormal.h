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
#include "ha3/sql/ops/agg/AggBase.h"
#include "ha3/sql/ops/agg/Aggregator.h"
#include "table/Table.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace navi {
class GraphMemoryPoolResource;
} // namespace navi

namespace isearch {
namespace sql {

class AggNormal : public AggBase {
public:
    AggNormal(navi::GraphMemoryPoolResource *memoryPoolResource,
              AggHints aggHints = AggHints());

private:
    AggNormal(const AggNormal &);
    AggNormal &operator=(const AggNormal &);

public:
    bool compute(table::TablePtr &input) override;
    table::TablePtr getTable() override;
    void getStatistics(uint64_t &collectTime,
                       uint64_t &outputAccTime,
                       uint64_t &mergeTime,
                       uint64_t &outputResultTime,
                       uint64_t &aggPoolSize) const override;

private:
    bool _aggregatorReady;
    Aggregator _normalAggregator;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggNormal> AggNormalPtr;
} // namespace sql
} // namespace isearch
