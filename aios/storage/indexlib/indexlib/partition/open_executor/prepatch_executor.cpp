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
#include "indexlib/partition/open_executor/prepatch_executor.h"

#include "autil/Lock.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/normal/attribute/virtual_attribute_data_appender.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

using namespace std;
using indexlib::index_base::PartitionDataPtr;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PrepatchExecutor);

#define IE_LOG_PREFIX mPartitionName.c_str()

PrepatchExecutor::PrepatchExecutor(autil::ThreadMutex* dataLock) : mDataLock(dataLock) {}

PrepatchExecutor::~PrepatchExecutor() {}

bool PrepatchExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetprepatchLatencyMetric().get());
    IE_LOG(INFO, "Begin prepatch execute for version [%d]", resource.mIncVersion.GetVersionId());
    resource.mSnapshotPartitionData.reset(resource.mPartitionDataHolder.Get()->Snapshot(mDataLock));

    BuildingPartitionDataPtr snapshotBuildingPartData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, resource.mSnapshotPartitionData);
    util::CounterMapPtr counterMap;
    BuildingPartitionParam param(resource.mOptions, resource.mRtSchema, resource.mPartitionMemController,
                                 snapshotBuildingPartData->GetDumpSegmentContainer(), counterMap,
                                 resource.mPluginManager, resource.mOnlinePartMetrics.GetMetricProvider(),
                                 resource.mSrcSignature);
    BuildingPartitionDataPtr partitionData // new inc with rt
        = PartitionDataCreator::CreateBranchBuildingPartitionData(
            param, snapshotBuildingPartData->GetSegmentDirectory(), resource.mIncVersion,
            snapshotBuildingPartData->GetInMemorySegment(), false);

    PatchLoaderPtr patchLoader = OpenExecutorUtil::CreatePatchLoader(resource, partitionData, mPartitionName, true);
    const util::MemoryReserverPtr memReserver = resource.mFileSystem->CreateMemoryReserver("prepatch_executor");
    auto expand = patchLoader->CalculatePatchLoadExpandSize();
    if (!memReserver->Reserve(expand)) {
        IE_PREFIX_LOG(WARN, "load reader fail, expand [%ld] over FreeQuota [%ld], version [%d to %d]", expand,
                      memReserver->GetFreeQuota(), resource.mLoadedIncVersion.GetVersionId(),
                      resource.mIncVersion.GetVersionId());
        return false;
    }
    index::VirtualAttributeDataAppender::PrepareVirtualAttrData(resource.mRtSchema, partitionData);
    resource.mBranchReader = OpenExecutorUtil::GetPatchedReader(resource, partitionData, patchLoader, mPartitionName);
    resource.mBranchPartitionDataHolder.Reset(partitionData);
    IE_LOG(INFO, "End prepatch execute for version [%d]", resource.mIncVersion.GetVersionId());
    return true;
}

void PrepatchExecutor::Drop(ExecutorResource& resource)
{
    resource.mBranchReader.reset();
    resource.mBranchPartitionDataHolder.Reset();
}

#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
