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
#include "navi/resource/GraphMemoryPoolR.h"
#include "kmonitor/client/MetricMacro.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/resource/MemoryPoolR.h"

using namespace kmonitor;

namespace navi {

class GraphGetPoolOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "GetPool.Qps");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, autil::mem_pool::Pool *pool) {
        REPORT_MUTABLE_QPS(_qps);
    }
private:
    MutableMetric *_qps = nullptr;
};

class GraphPutPoolOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "PutPool.Qps");
        REGISTER_GAUGE_MUTABLE_METRIC(_poolUsedBytes, "PutPool.PoolUsedBytes");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, autil::mem_pool::Pool *pool) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_poolUsedBytes, pool->getUsedBytes());
    }
private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_poolUsedBytes = nullptr;
};

const std::string GraphMemoryPoolR::RESOURCE_ID = "navi.buildin.resource.graph_mem_pool";

GraphMemoryPoolR::GraphMemoryPoolR() {
}

GraphMemoryPoolR::~GraphMemoryPoolR() {
    while (true) {
        autil::mem_pool::Pool *pool = nullptr;
        getPoolFromCache(pool);
        if (pool) {
            _memoryPoolR->putPool(pool);
        } else {
            break;
        }
    }
}

void GraphMemoryPoolR::def(ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, RS_GRAPH);
}

bool GraphMemoryPoolR::config(ResourceConfigContext &ctx) {
    return true;
}

ErrorCode GraphMemoryPoolR::init(ResourceInitContext &ctx) {
    _graphMemoryPoolReporter = _memoryPoolR->getGraphMemoryPoolReporter();
    return EC_NONE;
}

std::shared_ptr<autil::mem_pool::Pool> GraphMemoryPoolR::getPool() {
    auto thisPtr = std::dynamic_pointer_cast<GraphMemoryPoolR>(shared_from_this());
    auto pool = getPoolInner();
    if (_graphMemoryPoolReporter) {
        _graphMemoryPoolReporter->report<GraphGetPoolOpMetrics>(nullptr, pool);
    }
    return std::shared_ptr<autil::mem_pool::Pool>(pool, [thisPtr](autil::mem_pool::Pool *pool) mutable {
        thisPtr->putPool(pool);
        thisPtr.reset();
    });
}

void GraphMemoryPoolR::putPool(autil::mem_pool::Pool *pool) {
    if (_graphMemoryPoolReporter) {
        _graphMemoryPoolReporter->report<GraphPutPoolOpMetrics>(nullptr, pool);
    }
    if (unlikely(_memoryPoolR->reachReleaseThreshold(pool))) {
        _memoryPoolR->putPool(pool);
    } else {
        // cache && reuse within current query
        putPoolToCache(pool);
    }
}

autil::mem_pool::Pool *GraphMemoryPoolR::getPoolInner() {
    autil::mem_pool::Pool *pool;
    getPoolFromCache(pool);
    if (!pool) {
        pool = _memoryPoolR->getPool();
    }
    return pool;
}

REGISTER_RESOURCE(GraphMemoryPoolR);

}
