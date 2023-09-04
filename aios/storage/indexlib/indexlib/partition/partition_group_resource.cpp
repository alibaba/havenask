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
#include "indexlib/partition/partition_group_resource.h"

#include <limits>

#include "autil/EnvUtil.h"
#include "autil/ThreadPool.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/cache/SearchCache.h"
#include "indexlib/util/cache/SearchCacheCreator.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace indexlib::util;

namespace indexlib { namespace partition {

PartitionGroupResource::PartitionGroupResource() {}
PartitionGroupResource::~PartitionGroupResource() {}

PartitionGroupResourcePtr PartitionGroupResource::TEST_Create(int64_t totalQuota)
{
    return Create(totalQuota, kmonitor::MetricsReporterPtr(), "", "");
}

PartitionGroupResourcePtr PartitionGroupResource::Create(int64_t totalQuota,
                                                         const kmonitor::MetricsReporterPtr& reporter,
                                                         const char* fileBlockCacheParam, const char* searchCacheParam)
{
    util::MetricProviderPtr metricProvider(new util::MetricProvider(reporter));
    return Create(totalQuota, metricProvider, fileBlockCacheParam, searchCacheParam);
}

PartitionGroupResourcePtr PartitionGroupResource::Create(int64_t totalQuota,
                                                         const util::MetricProviderPtr& metricProvider,
                                                         const char* fileBlockCacheParam, const char* searchCacheParam)
{
    PartitionGroupResourcePtr resource(new PartitionGroupResource);

    resource->mMemoryQuotaController.reset(new indexlibv2::MemoryQuotaController(/*name*/ "_global_", totalQuota));
    resource->mTaskScheduler.reset(new util::TaskScheduler());
    resource->mMetricProvider = metricProvider;

    resource->mFileBlockCacheContainer.reset(new file_system::FileBlockCacheContainer());
    std::string fileBlockCacheParamStr = fileBlockCacheParam ? fileBlockCacheParam : "";
    if (!resource->mFileBlockCacheContainer->Init(fileBlockCacheParamStr, resource->mMemoryQuotaController,
                                                  resource->mTaskScheduler, resource->mMetricProvider)) {
        return PartitionGroupResourcePtr();
    }

    std::string searchCacheParamStr = searchCacheParam ? searchCacheParam : "";
    if (!searchCacheParamStr.empty()) {
        resource->mSearchCache.reset(
            util::SearchCacheCreator::Create(searchCacheParamStr, resource->mMemoryQuotaController,
                                             resource->mTaskScheduler, resource->mMetricProvider));
    }

    std::string memoryStatParam = autil::EnvUtil::getEnv("PRINT_MALLOC_STATS", "");
    MemoryStatReporterPtr memoryStatReporter(new MemoryStatReporter);
    if (memoryStatReporter->Init(memoryStatParam, resource->mSearchCache, resource->mFileBlockCacheContainer,
                                 resource->mTaskScheduler, resource->mMetricProvider)) {
        resource->mMemoryStatReporter = memoryStatReporter;
    }

    int64_t realtimeIndexSyncThreadNum = autil::EnvUtil::getEnv("REALTIME_INDEX_SYNC_THREAD_NUM", -1);
    if (realtimeIndexSyncThreadNum > 0) {
        resource->mRealtimeIndexSyncThreadPool.reset(new autil::ThreadPool(realtimeIndexSyncThreadNum));
        if (!resource->mRealtimeIndexSyncThreadPool->start("syncRealtimeIndex")) {
            return PartitionGroupResourcePtr();
        }
    }

    return resource;
}

// write
PartitionGroupResource&
PartitionGroupResource::SetMemoryQuotaController(const util::MemoryQuotaControllerPtr& memoryQuotaController)
{
    mMemoryQuotaController = memoryQuotaController;
    return *this;
}
PartitionGroupResource&
PartitionGroupResource::SetRealtimeQuotaController(const util::MemoryQuotaControllerPtr& realtimeQuotaController)
{
    mRealtimeQuotaController = realtimeQuotaController;
    return *this;
}
PartitionGroupResource& PartitionGroupResource::SetPartitionGroupName(const std::string& partitionGroupName)
{
    mPartitionGroupName = partitionGroupName;
    return *this;
}

}} // namespace indexlib::partition
