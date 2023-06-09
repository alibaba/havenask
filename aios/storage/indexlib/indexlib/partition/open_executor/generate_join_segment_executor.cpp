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
#include "indexlib/partition/open_executor/generate_join_segment_executor.h"

#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/join_segment_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, GenerateJoinSegmentExecutor);

bool GenerateJoinSegmentExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetGenerateJoinSegmentLatencyMetric().get());
    IE_LOG(INFO, "generate join segment begin");
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
    bool ret = mJoinSegWriter->Join() && mJoinSegWriter->Dump(resource.mPartitionDataHolder.Get());
    mJoinSegWriter.reset();
    IE_LOG(INFO, "generate join segment end");
    return ret;
}
void GenerateJoinSegmentExecutor::Drop(ExecutorResource& resource)
{
    if (resource.mForceReopen) {
        return;
    }
    resource.mPartitionDataHolder.Reset(mOriginalPartitionData);
    OnlineSegmentDirectoryPtr segDir =
        DYNAMIC_POINTER_CAST(OnlineSegmentDirectory, mOriginalPartitionData->GetSegmentDirectory());
    SegmentDirectoryPtr joinSegmentDir = segDir->GetJoinSegmentDirectory();
    joinSegmentDir->RollbackToCurrentVersion();
    mOriginalPartitionData.reset();
    mJoinSegWriter.reset();
}
}} // namespace indexlib::partition
