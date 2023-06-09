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

#include "autil/LoopThread.h"

#include "navi/resource/MemoryPoolResourceBase.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace navi {

class MemoryPoolResource : public MemoryPoolResourceBase {
public:
    MemoryPoolResource();
    ~MemoryPoolResource();

private:
    MemoryPoolResource(const MemoryPoolResource &);
    MemoryPoolResource &operator=(const MemoryPoolResource &);

public:
    void def(ResourceDefBuilder &builder) const override;
    bool config(ResourceConfigContext &ctx) override;
    ErrorCode init(ResourceInitContext &ctx) override;

public:
    bool init(std::shared_ptr<kmonitor::MetricsReporter> metricsReporter);

public:
    autil::mem_pool::Pool *getPool();
    void putPool(autil::mem_pool::Pool *pool);
    kmonitor::MetricsReporter *getGraphMemoryPoolReporter() { return _graphMemoryPoolReporter.get(); }
    bool reachReleaseThreshold(autil::mem_pool::Pool *pool) const {
        return _useAsanPool || (_poolReleaseThreshold != 0 && pool->getTotalBytes() > _poolReleaseThreshold);
    }

private:
    void initConfigParams();
    void initMetricsReporter(kmonitor::MetricsReporter &baseReporter);
    bool reachPoolCacheSizeLimit();
    void poolCacheSizeAutoScale();
    bool myHasInterceptorMalloc();

private:
    static constexpr size_t DEFAULT_POOL_CHUNK_SIZE = 2;         // 2MB
    static constexpr size_t DEFAULT_POOL_RELEASE_THRESHOLD = 16; // 16MB
    static constexpr size_t DEFAULT_POOL_RELEASE_RATE = 3ul;                   // rate = 3 / 13
    static constexpr size_t DEFAULT_POOL_CACHE_AUTOSCALE_KEEP_COUNT = 500ul;

private:
    DECLARE_LOGGER();
    bool _useAsanPool;
    size_t _poolChunkSize;
    size_t _poolReleaseThreshold;
    size_t _poolCacheAutoScaleKeepCount;
    size_t _poolAutoScaleReleaseRate; // range [0..10]
private:
    std::shared_ptr<kmonitor::MetricsReporter> _commonReporter;
    std::shared_ptr<kmonitor::MetricsReporter> _graphMemoryPoolReporter;
    std::shared_ptr<kmonitor::MetricsReporter> _getPoolOpByNewReporter;
    std::shared_ptr<kmonitor::MetricsReporter> _getPoolOpByCacheReporter;
    std::shared_ptr<kmonitor::MetricsReporter> _putPoolOpDeletedByThresholdReporter;
    std::shared_ptr<kmonitor::MetricsReporter> _putPoolOpDeletedByPoolCacheSizeLimitReporter;
    std::shared_ptr<kmonitor::MetricsReporter> _putPoolOpRecycleReporter;

private:
    std::atomic<size_t> _poolCacheSizeLB;
    std::atomic<size_t> _poolCacheSizeUB;
    std::atomic<size_t> _poolCacheSizeLimit;
    autil::LoopThreadPtr _poolCacheSizeAutoScaleThread;
};

NAVI_TYPEDEF_PTR(MemoryPoolResource);

} // namespace navi
