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
#include "indexlib/partition/open_executor/reclaim_rt_segments_executor.h"

#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReclaimRtSegmentsExecutor);

ReclaimRtSegmentsExecutor::ReclaimRtSegmentsExecutor(bool needReclaimBuildingSegment)
    : mNeedReclaimBuildingSegment(needReclaimBuildingSegment)
{
}

ReclaimRtSegmentsExecutor::~ReclaimRtSegmentsExecutor() {}

bool ReclaimRtSegmentsExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetReclaimRtSegmentLatencyMetric().get());
    IE_LOG(INFO, "reclaim realtime segments begin");
    if (!resource.mForceReopen) {
        mOriginalPartitionData = resource.mPartitionDataHolder.Get();
    }
    InMemorySegmentPtr orgInMemSegment;
    if (resource.mNeedInheritInMemorySegment) {
        orgInMemSegment = resource.mPartitionDataHolder.Get()->GetInMemorySegment();
    }
    util::CounterMapPtr counterMap;
    BuildingPartitionParam param(resource.mOptions, resource.mRtSchema, resource.mPartitionMemController,
                                 resource.mDumpSegmentContainer, counterMap, resource.mPluginManager,
                                 resource.mOnlinePartMetrics.GetMetricProvider(), resource.mSrcSignature);
    resource.mPartitionDataHolder.Reset(PartitionDataCreator::CreateBuildingPartitionData(
        param, resource.mFileSystem, resource.mLoadedIncVersion, "", orgInMemSegment));

    OnlineJoinPolicy joinPolicy(resource.mIncVersion, resource.mRtSchema->GetTableType(), resource.mSrcSignature);
    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();

    RealtimePartitionDataReclaimer reclaimer(resource.mRtSchema, resource.mOptions);
    reclaimer.TrimObsoleteAndEmptyRtSegments(reclaimTimestamp, resource.mPartitionDataHolder.Get());
    if (mNeedReclaimBuildingSegment) {
        reclaimer.TrimBuildingSegment(reclaimTimestamp, resource.mPartitionDataHolder.Get());
    }
    IE_LOG(INFO, "reclaim realtime segments end");
    return true;
}

void ReclaimRtSegmentsExecutor::Drop(ExecutorResource& resource)
{
    if (resource.mForceReopen) {
        return;
    }
    resource.mPartitionDataHolder.Reset(mOriginalPartitionData);
    OnlineSegmentDirectoryPtr segDir =
        DYNAMIC_POINTER_CAST(OnlineSegmentDirectory, mOriginalPartitionData->GetSegmentDirectory());
    SegmentDirectoryPtr rtSegmentDir = segDir->GetRtSegmentDirectory();
    rtSegmentDir->RollbackToCurrentVersion(false);
    mOriginalPartitionData.reset();
}
}} // namespace indexlib::partition
