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
#include "indexlib/util/cache/SearchCache.h"

#include "autil/EnvUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/cache/SearchCacheTaskItem.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, SearchCache);

SearchCache::SearchCache(size_t cacheSize, const MemoryQuotaControllerPtr& memoryQuotaController,
                         const TaskSchedulerPtr& taskScheduler, MetricProviderPtr metricProvider, int numShardBits,
                         float highPriorityRatio)
    : _cache(autil::NewLRUCache(cacheSize, numShardBits, true, highPriorityRatio, autil::CacheAllocatorPtr()))
    , _cacheSize(cacheSize)
    , _reportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
{
    assert(_cache);
    Init(memoryQuotaController, taskScheduler, metricProvider);
}

SearchCache::~SearchCache()
{
    if (_memoryQuotaController) {
        _memoryQuotaController->Free(_cacheSize);
    }
    if (_taskScheduler) {
        _taskScheduler->DeleteTask(_reportMetricsTaskId);
    }
}

void SearchCache::Init(const MemoryQuotaControllerPtr& memoryQuotaController, const TaskSchedulerPtr& taskScheduler,
                       MetricProviderPtr metricProvider)
{
    _memoryQuotaController = memoryQuotaController;
    if (_memoryQuotaController) {
        _memoryQuotaController->Allocate(_cacheSize);
    }

    _taskScheduler = taskScheduler;
    RegisterMetricsReporter(metricProvider);
}

bool SearchCache::RegisterMetricsReporter(util::MetricProviderPtr metricProvider)
{
    IE_INIT_METRIC_GROUP(metricProvider, SearchCacheHitRatio, "global/SearchCacheHitRatio", kmonitor::GAUGE, "%");
    IE_INIT_LOCAL_INPUT_METRIC(_hitRatioReporter, SearchCacheHitRatio);
    IE_INIT_METRIC_GROUP(metricProvider, SearchCacheHitQps, "global/SearchCacheHitQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(metricProvider, SearchCacheMissQps, "global/SearchCacheMissQps", kmonitor::QPS, "count");

    _hitReporter.Init(mSearchCacheHitQpsMetric);
    _missReporter.Init(mSearchCacheMissQpsMetric);

    _hitReporter.Init(mSearchCacheHitQpsMetric);
    _missReporter.Init(mSearchCacheMissQpsMetric);

    if (!_taskScheduler || !metricProvider) {
        return true;
    }
    int32_t sleepTime =
        autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false) ? REPORT_METRICS_INTERVAL / 1000 : REPORT_METRICS_INTERVAL;
    if (!_taskScheduler->DeclareTaskGroup("report_metrics", sleepTime)) {
        AUTIL_LOG(ERROR, "search cache declare report metrics task failed!");
        return false;
    }
    TaskItemPtr reportMetricsTask(new SearchCacheTaskItem(this, metricProvider));
    _reportMetricsTaskId = _taskScheduler->AddTask("report_metrics", reportMetricsTask);
    if (_reportMetricsTaskId == TaskScheduler::INVALID_TASK_ID) {
        AUTIL_LOG(ERROR, "search cache add report metrics task failed!");
        return false;
    }
    return true;
}
}} // namespace indexlib::util
