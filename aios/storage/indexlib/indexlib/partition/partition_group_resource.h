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

#include <limits>
#include <memory>
#include <string>

#include "autil/CommonMacros.h"

AUTIL_DECLARE_CLASS_SHARED_PTR(autil, ThreadPool);
AUTIL_DECLARE_CLASS_SHARED_PTR(kmonitor, MetricsReporter);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::util, TaskScheduler);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::util, SearchCache);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::file_system, FileBlockCacheContainer);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::util, MetricProvider);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::partition, MemoryStatReporter);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::partition, PartitionGroupResource);

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlib { namespace partition {

class PartitionGroupResource
{
public:
    PartitionGroupResource();
    ~PartitionGroupResource();

public:
    static PartitionGroupResourcePtr Create(int64_t totalQuota, const util::MetricProviderPtr& reporter,
                                            const char* fileBlockCacheParam, const char* searchCacheParam);
    static PartitionGroupResourcePtr Create(int64_t totalQuota, const kmonitor::MetricsReporterPtr& reporter,
                                            const char* fileBlockCacheParam, const char* searchCacheParam);

public:
    static PartitionGroupResourcePtr TEST_Create(int64_t totalQuota = std::numeric_limits<int64_t>::max());

public:
    // write
    PartitionGroupResource&
    SetMemoryQuotaController(const std::shared_ptr<indexlibv2::MemoryQuotaController>& memoryQuotaController);
    PartitionGroupResource&
    SetRealtimeQuotaController(const std::shared_ptr<indexlibv2::MemoryQuotaController>& realtimeQuotaController);
    PartitionGroupResource& SetPartitionGroupName(const std::string& partitionGroupName);

public:
    // read
    const std::shared_ptr<indexlibv2::MemoryQuotaController>& GetMemoryQuotaController() const
    {
        return mMemoryQuotaController;
    }
    const std::shared_ptr<indexlibv2::MemoryQuotaController>& GetRealtimeQuotaController() const
    {
        return mRealtimeQuotaController;
    }
    const util::TaskSchedulerPtr& GetTaskScheduler() const { return mTaskScheduler; }
    const util::SearchCachePtr& GetSearchCache() const { return mSearchCache; }
    const MemoryStatReporterPtr& GetMemoryStatReporter() const { return mMemoryStatReporter; }
    const autil::ThreadPoolPtr& GetRealtimeIndexSyncThreadPool() const { return mRealtimeIndexSyncThreadPool; }
    const util::MetricProviderPtr GetMetricProvider() const { return mMetricProvider; }
    const file_system::FileBlockCacheContainerPtr& GetFileBlockCacheContainer() const
    {
        return mFileBlockCacheContainer;
    }
    const std::string& GetPartitionGroupName() const { return mPartitionGroupName; }

private:
    std::shared_ptr<indexlibv2::MemoryQuotaController> mMemoryQuotaController = nullptr; // required
    std::shared_ptr<indexlibv2::MemoryQuotaController> mRealtimeQuotaController = nullptr;
    util::TaskSchedulerPtr mTaskScheduler = nullptr; // required
    util::SearchCachePtr mSearchCache = nullptr;
    file_system::FileBlockCacheContainerPtr mFileBlockCacheContainer = nullptr;
    util::MetricProviderPtr mMetricProvider = nullptr;
    MemoryStatReporterPtr mMemoryStatReporter = nullptr;
    autil::ThreadPoolPtr mRealtimeIndexSyncThreadPool = nullptr;
    std::string mPartitionGroupName;
};
}} // namespace indexlib::partition
