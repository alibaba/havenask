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
#include "indexlib/partition/open_executor/switch_branch_executor.h"

#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/realtime_partition_data_reclaimer_base.h"

using namespace std;

namespace indexlib { namespace partition {
using namespace indexlib::index;
using namespace indexlib::index_base;
IE_LOG_SETUP(partition, SwitchBranchExecutor);

SwitchBranchExecutor::~SwitchBranchExecutor() {}

bool SwitchBranchExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetSwitchBranchLatencyMetric().get());
    IE_LOG(INFO, "Begin reopen switch branch exectuor for version [%d]", resource.mIncVersion.GetVersionId());
    BuildingPartitionParam param(resource.mOptions, resource.mRtSchema, resource.mPartitionMemController,
                                 resource.mDumpSegmentContainer, resource.mCounterMap, resource.mPluginManager,
                                 resource.mOnlinePartMetrics.GetMetricProvider(), resource.mSrcSignature);
    BuildingPartitionDataPtr newPartitionData = PartitionDataCreator::CreateBuildingPartitionDataWithoutInMemSegment(
        param, resource.mFileSystem, resource.mIncVersion, "");

    MergeDeletionMap(resource, newPartitionData);
    OpenExecutorUtil::InitReader(resource, newPartitionData, nullptr, mPartitionName, resource.mBranchReader.get());
    OpenExecutorUtil::InitWriter(resource, newPartitionData);
    resource.mPartitionDataHolder.Reset(newPartitionData);
    resource.mReader->EnableAccessCountors(resource.mNeedReportTemperatureAccess);

    // after the new reader is switched, mLoadedIncVersion can be updated
    resource.mLoadedIncVersion = resource.mIncVersion;
    IE_LOG(INFO, "End reopen switch branch exectuor for version [%d]", resource.mIncVersion.GetVersionId());
    return true;
}

void SwitchBranchExecutor::MergeDeletionMap(ExecutorResource& resource, const BuildingPartitionDataPtr& partitionData)
{
    IE_LOG(INFO, "merge deletion map begin");

    DeletionMapWriterPtr fileDeletionMapWriter(new DeletionMapWriter(false)); // file adreess
    fileDeletionMapWriter->Init(partitionData.get());
    auto branchReader = resource.mBranchPartitionDataHolder.Get()->GetDeletionMapReader();
    DeletionMapWriterPtr subFileDeletionMapWriter(new DeletionMapWriter(false));
    index::DeletionMapReaderPtr subDeletionMapReader(new DeletionMapReader());
    if (resource.mSchema->GetSubIndexPartitionSchema() != nullptr) {
        subFileDeletionMapWriter->Init(partitionData->GetSubPartitionData().get());
        subDeletionMapReader = resource.mBranchPartitionDataHolder.Get()->GetSubPartitionData()->GetDeletionMapReader();
    }
    auto segIter = resource.mBranchPartitionDataHolder.Get()->CreateSegmentIterator();
    while (segIter->IsValid()) {
        segmentid_t segId = segIter->GetSegmentId();
        auto fileWriter = fileDeletionMapWriter->GetSegmentWriter(segId);
        if (!fileWriter) {
            segIter->MoveToNext();
            continue;
        }
        IE_LOG(INFO, "merge bitmap for segment [%d] begin", segId);
        bool ret = fileWriter->MergeBitmap(branchReader->GetSegmentDeletionMap(segId), false);
        (void)ret;
        assert(ret);
        if (resource.mSchema->GetSubIndexPartitionSchema() != nullptr) {
            auto subFileWriter = subFileDeletionMapWriter->GetSegmentWriter(segId);
            if (subFileWriter) {
                ret = subFileWriter->MergeBitmap(subDeletionMapReader->GetSegmentDeletionMap(segId), false);
                (void)ret;
                assert(ret);
            }
        }
        IE_LOG(INFO, "merge bitmap for segment [%d] end", segId);
        segIter->MoveToNext();
    }
    TrimObsoleteAndEmptyRtSegments(resource.mBranchPartitionDataHolder.Get(), partitionData);
    if (resource.mPartitionDataHolder.Get()->GetInMemorySegment()) {
        partitionData->SwitchPrivateInMemSegment(resource.mPartitionDataHolder.Get()->GetInMemorySegment());
    }
    auto holder = resource.mBranchPartitionDataHolder;
    if (partitionData->GetInMemorySegment() && holder.Get()->GetInMemorySegment() &&
        partitionData->GetInMemorySegment()->GetSegmentId() == holder.Get()->GetInMemorySegment()->GetSegmentId()) {
        // segment is building in both branch partition and primary partition
        auto deletionMapReader = holder.Get()->GetDeletionMapReader();
        bool ret = partitionData->GetDeletionMapReader()->MergeBuildingBitmap(*deletionMapReader);
        if (resource.mSchema->GetSubIndexPartitionSchema() != nullptr) {
            auto subDeletionMap = holder.Get()->GetSubPartitionData()->GetDeletionMapReader();
            partitionData->GetSubPartitionData()->GetDeletionMapReader()->MergeBuildingBitmap(*subDeletionMap);
        }
        assert(ret);
        (void)ret;
    }
    auto inmemSegment = partitionData->GetInMemorySegment();
    if (inmemSegment) {
        auto partInfo = partitionData->GetPartitionInfo();
        partInfo->UpdateInMemorySegment(inmemSegment);
    }

    IE_LOG(INFO, "merge deletion map end");
}

void SwitchBranchExecutor::TrimObsoleteAndEmptyRtSegments(const index_base::PartitionDataPtr& branchPartitionData,
                                                          const index_base::PartitionDataPtr& currentPartitionData)
{
    IE_LOG(INFO, "trim empty rt segment begin");
    Version diffVersion = currentPartitionData->GetVersion() - branchPartitionData->GetVersion();
    Version::SegmentIdVec segments = diffVersion.GetSegmentVector();
    vector<segmentid_t> segIdsToRemove;
    assert(branchPartitionData->GetInMemorySegment() != nullptr);
    segmentid_t inMemSegmentId = branchPartitionData->GetInMemorySegment()->GetSegmentId();
    for (auto segment : segments) {
        if (RealtimeSegmentDirectory::IsRtSegmentId(segment) && segment < inMemSegmentId) {
            segIdsToRemove.push_back(segment);
        }
    }
    if (segIdsToRemove.empty()) {
        return;
    }
    currentPartitionData->RemoveSegments(segIdsToRemove);
    IE_LOG(INFO, "trim empty rt segment end");
}

void SwitchBranchExecutor::Drop(ExecutorResource& resource)
{
    // drop的时候需要把原来的deletionMap改回去，目前这个executor是最后一步，没有drop的场景
    // 同时countermap也需要回滚
    // and release the lock
    IE_LOG(ERROR, "drop is not support now");
    assert(false);
}
}} // namespace indexlib::partition
