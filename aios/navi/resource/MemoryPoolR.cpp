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
#include "navi/resource/MemoryPoolR.h"

#include "autil/EnvUtil.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "lockless_allocator/LocklessApi.h"
#include "navi/builder/ResourceDefBuilder.h"

using namespace kmonitor;
using namespace std;

namespace navi {

static const std::string POOL_MODE_ASAN = "naviPoolModeAsan";
static const std::string POOL_CACHE_AUTOSCALE_KEEP_COUNT = "naviPoolAutoScaleKeepCount";
static const std::string POOL_TRUNK_SIZE = "naviPoolTrunkSize";
static const std::string POOL_RECYCLE_SIZE_LIMIT = "naviPoolRecycleSizeLimit";
static const std::string POOL_RELEASE_RATE = "naviPoolReleaseRate";
static const std::string POOL_CACHE_SIZE_METRIC = "poolCacheSize";
static const std::string POOL_CACHE_SIZE_LIMIT_METRIC = "poolCacheSizeLimit";
static constexpr size_t POOL_AUTO_SCALE_SECONDS = 180;

class GetPoolOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "Qps");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, autil::mem_pool::Pool *pool) {
        REPORT_MUTABLE_QPS(_qps);
    }
private:
    MutableMetric *_qps = nullptr;
};

class PutPoolOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "Qps");
        REGISTER_GAUGE_MUTABLE_METRIC(_poolTotalBytes, "PoolTotalBytes");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, autil::mem_pool::Pool *pool) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_poolTotalBytes, pool->getTotalBytes());
    }
private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_poolTotalBytes = nullptr;
};

const std::string MemoryPoolR::RESOURCE_ID = "navi.buildin.resource.mem_pool";

MemoryPoolR::MemoryPoolR()
    : _useAsanPool(autil::EnvUtil::getEnv(POOL_MODE_ASAN, myHasInterceptorMalloc()))
    , _poolChunkSize(autil::EnvUtil::getEnv(POOL_TRUNK_SIZE, DEFAULT_POOL_CHUNK_SIZE) * 1024 * 1024)
    , _poolReleaseThreshold(autil::EnvUtil::getEnv(POOL_RECYCLE_SIZE_LIMIT, DEFAULT_POOL_RELEASE_THRESHOLD) * 1024 *
                            1024)
    , _poolCacheAutoScaleKeepCount(
          autil::EnvUtil::getEnv(POOL_CACHE_AUTOSCALE_KEEP_COUNT, DEFAULT_POOL_CACHE_AUTOSCALE_KEEP_COUNT))
    , _poolAutoScaleReleaseRate(autil::EnvUtil::getEnv(POOL_RELEASE_RATE, DEFAULT_POOL_RELEASE_RATE))
    , _poolCacheSizeLB(std::numeric_limits<size_t>::max())
    , _poolCacheSizeUB(std::numeric_limits<size_t>::min())
    , _poolCacheSizeLimit(0ul) {}

MemoryPoolR::~MemoryPoolR() {
}

bool MemoryPoolR::config(ResourceConfigContext &ctx) {
    assert(false);
    return false;
}

ErrorCode MemoryPoolR::init(ResourceInitContext &ctx) {
    assert(false);
    return EC_INIT_RESOURCE;
}

void MemoryPoolR::def(ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, RS_EXTERNAL);
}

void MemoryPoolR::initConfigParams() {
    if (_poolReleaseThreshold != 0 && _poolReleaseThreshold < _poolChunkSize) {
        NAVI_KERNEL_LOG(WARN,
                        "original poolReleaseThreshold[%lu] < poolChunkSize[%lu]"
                        ", value changed",
                        _poolReleaseThreshold,
                        _poolChunkSize);
        _poolReleaseThreshold = _poolChunkSize;
    }
    if (_poolAutoScaleReleaseRate > 10ul) {
        NAVI_KERNEL_LOG(WARN, "original poolAutoScaleReleaseRate[%lu] > 10, force to[10]", _poolAutoScaleReleaseRate);
        _poolAutoScaleReleaseRate = 10ul;
    }
    NAVI_KERNEL_LOG(INFO,
                    "memory pool resource config finished: "
                    "poolChunkSize[%lu] "
                    "poolReleaseThreshold[%lu] poolAutoScaleReleaseRate[%lu] keepCount[%lu] useAsanPool[%d]",
                    _poolChunkSize,
                    _poolReleaseThreshold,
                    _poolAutoScaleReleaseRate,
                    _poolCacheAutoScaleKeepCount,
                    _useAsanPool);
    _poolCacheSizeLimit = 0ul;
}

bool MemoryPoolR::init(kmonitor::MetricsReporterPtr metricsReporter) {
    initLogger(_logger, "NaviMemScale");
    initConfigParams();
    if (metricsReporter) {
        initMetricsReporter(*metricsReporter);
    } else {
        NAVI_KERNEL_LOG(INFO, "no metrics reporter provided, skip init metrics reporter");
    }
    _poolCacheSizeAutoScaleThread =
        autil::LoopThread::createLoopThread(std::bind(&MemoryPoolR::poolCacheSizeAutoScale, this),
                                            POOL_AUTO_SCALE_SECONDS * 1000 * 1000,
                                            "NaviMemSc");
    if (_poolCacheSizeAutoScaleThread == nullptr) {
        NAVI_KERNEL_LOG(ERROR, "init pool cache size auto scale thread failed");
        return false;
    }
    return true;
}

std::shared_ptr<autil::mem_pool::Pool> MemoryPoolR::getPoolPtr() {
    auto thisPtr = std::dynamic_pointer_cast<MemoryPoolR>(shared_from_this());
    auto pool = getPool();
    return std::shared_ptr<autil::mem_pool::Pool>(pool, [thisPtr](autil::mem_pool::Pool *pool) mutable {
        thisPtr->putPool(pool);
        thisPtr.reset();
    });
}

autil::mem_pool::Pool *MemoryPoolR::getPool() {
    autil::mem_pool::Pool *pool;

    size_t poolCacheSize = getPoolFromCache(pool);
    if (poolCacheSize > _poolCacheSizeUB.load(std::memory_order_relaxed)) {
        _poolCacheSizeUB.store(poolCacheSize, std::memory_order_relaxed);
    }
    if (poolCacheSize < _poolCacheSizeLB.load(std::memory_order_relaxed)) {
        _poolCacheSizeLB.store(poolCacheSize, std::memory_order_relaxed);
    }

    if (pool) {
        if (_getPoolOpByCacheReporter) {
            _getPoolOpByCacheReporter->report<GetPoolOpMetrics>(nullptr, pool);
        }
    } else {
        size_t limit = _poolCacheSizeLimit.fetch_add(1, std::memory_order_relaxed);
        REPORT_USER_MUTABLE_STATUS(_commonReporter, POOL_CACHE_SIZE_LIMIT_METRIC, limit);

        if (_useAsanPool) {
            pool = new autil::mem_pool::PoolAsan();
        } else {
            pool = new autil::mem_pool::Pool(_poolChunkSize);
        }
        if (_getPoolOpByNewReporter) {
            _getPoolOpByNewReporter->report<GetPoolOpMetrics>(nullptr, pool);
        }
    }
    REPORT_USER_MUTABLE_METRIC(_commonReporter, POOL_CACHE_SIZE_METRIC, poolCacheSize);
    return pool;
}

void MemoryPoolR::putPool(autil::mem_pool::Pool *pool) {
    if (reachReleaseThreshold(pool)) {
        if (_putPoolOpDeletedByThresholdReporter) {
            _putPoolOpDeletedByThresholdReporter->report<PutPoolOpMetrics>(nullptr, pool);
        }
        delete pool;
    } else if (reachPoolCacheSizeLimit()) {
        if (_putPoolOpDeletedByPoolCacheSizeLimitReporter) {
            _putPoolOpDeletedByPoolCacheSizeLimitReporter->report<PutPoolOpMetrics>(
                    nullptr, pool);
        }
        delete pool;
    } else {
        if (_putPoolOpRecycleReporter) {
            _putPoolOpRecycleReporter->report<PutPoolOpMetrics>(nullptr, pool);
        }
        size_t poolCacheSize = putPoolToCache(pool);
        REPORT_USER_MUTABLE_METRIC(_commonReporter, POOL_CACHE_SIZE_METRIC, poolCacheSize);
    }
}

void MemoryPoolR::initMetricsReporter(kmonitor::MetricsReporter &baseReporter) {
    _commonReporter = baseReporter.getSubReporter("memory_pool", {});
    _graphMemoryPoolReporter = _commonReporter->getSubReporter("graph", {});
    _getPoolOpByNewReporter = _commonReporter->getSubReporter(
            "GetPool", {"type", "ByNew"});
    _getPoolOpByCacheReporter = _commonReporter->getSubReporter(
            "GetPool", {"type", "ByCache"});
    _putPoolOpDeletedByThresholdReporter = _commonReporter->getSubReporter(
            "PutPool", {"type", "DeletedByThreshold"});
    _putPoolOpDeletedByPoolCacheSizeLimitReporter = _commonReporter->getSubReporter(
            "PutPool", {"type", "DeletedByPoolCacheSizeLimit"});
    _putPoolOpRecycleReporter = _commonReporter->getSubReporter(
            "PutPool", {"type", "Recycle"});
    NAVI_KERNEL_LOG(INFO, "finish init metrics reporter");
}

bool MemoryPoolR::reachPoolCacheSizeLimit() {
    size_t poolCacheSizeLimit = _poolCacheSizeLimit.load(std::memory_order_relaxed);
    return _poolQueue.Size() + 1 > poolCacheSizeLimit;
}

void MemoryPoolR::poolCacheSizeAutoScale() {
    size_t poolCacheSizeLB = _poolCacheSizeLB.load();
    size_t poolCacheSizeUB = _poolCacheSizeUB.load();
    _poolCacheSizeLB.store(std::numeric_limits<size_t>::max());
    _poolCacheSizeUB.store(std::numeric_limits<size_t>::min());

    size_t poolCacheSizeLimit = 0;
    if (poolCacheSizeUB >= poolCacheSizeLB) {
        // to be released pools: poolCacheSizeLB, but add a smooth factor
        poolCacheSizeLimit =
            poolCacheSizeUB - poolCacheSizeLB * _poolAutoScaleReleaseRate / (10 + _poolAutoScaleReleaseRate);
    }
    poolCacheSizeLimit = std::max(poolCacheSizeLimit, _poolCacheAutoScaleKeepCount);
    _poolCacheSizeLimit.store(poolCacheSizeLimit);

    REPORT_USER_MUTABLE_STATUS(_commonReporter,
                               POOL_CACHE_SIZE_LIMIT_METRIC, poolCacheSizeLimit);
    NAVI_LOG(DEBUG, "pool cache size limit updated, lb [%lu] ub [%lu] limit [%lu]",
             poolCacheSizeLB, poolCacheSizeUB, poolCacheSizeLimit);
}

bool MemoryPoolR::myHasInterceptorMalloc() {
    return hasInterceptorMalloc();
}

REGISTER_RESOURCE(MemoryPoolR);

}
