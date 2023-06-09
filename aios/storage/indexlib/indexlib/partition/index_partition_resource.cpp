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
#include "indexlib/partition/index_partition_resource.h"

#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/partition/partition_group_resource.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlib { namespace partition {
IndexPartitionResource::IndexPartitionResource() {}

IndexPartitionResource::IndexPartitionResource(const PartitionGroupResource& partitionGroupResource,
                                               const std::string& partitionName_)
{
    partitionName = partitionName_;

    // partition group resource
    if (partitionGroupResource.GetSearchCache()) {
        searchCache.reset(
            new util::SearchCachePartitionWrapper(partitionGroupResource.GetSearchCache(), partitionName));
    }
    memoryQuotaController = partitionGroupResource.GetMemoryQuotaController();
    realtimeQuotaController = partitionGroupResource.GetRealtimeQuotaController();
    taskScheduler = partitionGroupResource.GetTaskScheduler();
    fileBlockCacheContainer = partitionGroupResource.GetFileBlockCacheContainer();
    memoryStatReporter = partitionGroupResource.GetMemoryStatReporter();
    realtimeIndexSyncThreadPool = partitionGroupResource.GetRealtimeIndexSyncThreadPool();
    metricProvider = partitionGroupResource.GetMetricProvider();
    partitionGroupName = partitionGroupResource.GetPartitionGroupName();
}

IndexPartitionResource IndexPartitionResource::Create(IRCreateType cType, int64_t totalQuota)
{
    IndexPartitionResource indexPartitionResource;
    indexPartitionResource.memoryQuotaController =
        std::make_shared<indexlibv2::MemoryQuotaController>(/*name*/ "", totalQuota);
    indexPartitionResource.taskScheduler.reset(new util::TaskScheduler());

    switch (cType) {
    case IR_OFFLINE_LEGACY:
        indexPartitionResource.branchOption = index_base::CommonBranchHinterOption::Legacy(0, "");
        break;
    case IR_OFFLINE_TEST:
        indexPartitionResource.branchOption = index_base::CommonBranchHinterOption::Test();
        break;
    case IR_ONLINE:
    case IR_UNKNOWN:
    default:
        break;
    }
    return indexPartitionResource;
}

IndexPartitionResource::~IndexPartitionResource() {}

}} // namespace indexlib::partition
