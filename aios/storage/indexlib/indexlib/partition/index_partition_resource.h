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

#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h" // for ParallelBuildInfo
#include "indexlib/indexlib.h"                                  // for PartitionRange
#include "indexlib/partition/partition_group_resource.h"

AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::util, SearchCachePartitionWrapper);

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlib { namespace partition {

class IndexPartitionResource;
typedef std::shared_ptr<IndexPartitionResource> IndexPartitionResourcePtr;

// TODO: use have partitionGroupResource instead of copy
class IndexPartitionResource
{
public:
    IndexPartitionResource();
    IndexPartitionResource(const PartitionGroupResource& partitionGroupResource, const std::string& partitionName);
    ~IndexPartitionResource();

public:
    enum IRCreateType {
        IR_UNKNOWN,
        IR_ONLINE,
        IR_OFFLINE_LEGACY,
        IR_OFFLINE_TEST,
    };
    static IndexPartitionResource Create(IRCreateType cType, int64_t totalQuota = std::numeric_limits<int64_t>::max());

public:
    std::shared_ptr<indexlibv2::MemoryQuotaController> memoryQuotaController;
    std::shared_ptr<indexlibv2::MemoryQuotaController> realtimeQuotaController;
    util::TaskSchedulerPtr taskScheduler;
    file_system::FileBlockCacheContainerPtr fileBlockCacheContainer;
    autil::ThreadPoolPtr realtimeIndexSyncThreadPool;
    MemoryStatReporterPtr memoryStatReporter;
    util::SearchCachePartitionWrapperPtr searchCache;
    util::MetricProviderPtr metricProvider;
    std::string indexPluginPath;
    std::string partitionName;
    // online only
    std::string partitionGroupName;
    // offline only
    index_base::ParallelBuildInfo parallelBuildInfo;
    PartitionRange range;
    document::SrcSignature srcSignature;
    index_base::CommonBranchHinterOption branchOption;
};

}} // namespace indexlib::partition
