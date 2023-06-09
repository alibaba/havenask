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
#ifndef __INDEXLIB_KV_MERGE_WORK_ITEM_H
#define __INDEXLIB_KV_MERGE_WORK_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_work_item.h"

namespace indexlib { namespace merger {

template <typename MergerPtr>
class KVMergeWorkItem : public MergeWorkItem
{
public:
    KVMergeWorkItem(MergerPtr& merger, const merger::MergeFileSystemPtr& mergeFileSystem,
                    const std::string& segmentPath, const std::string& indexPath,
                    const index_base::SegmentDataVector& segmentDataVec,
                    const index_base::SegmentTopologyInfo& targetTopoInfo, int64_t estimateMemUse = -1);

public:
    void DoProcess() override;
    void Destroy() override;
    int64_t GetRequiredResource() const override { return mRequiredResource; }
    int64_t EstimateMemoryUse() const override { return mRequiredResource; }

    void SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics) override;

private:
    int64_t DoEstimateMemoryUse() const;
    void processForNormal();

private:
    MergerPtr mMerger;
    std::string mSegmentPath;
    std::string mIndexPath;
    index_base::SegmentDataVector mSegmentDataVec;
    index_base::SegmentTopologyInfo mTargetTopoInfo;
    int64_t mRequiredResource;

public:
private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(merger, KVMergeWorkItem);

template <class MergerPtr>
KVMergeWorkItem<MergerPtr>::KVMergeWorkItem(MergerPtr& merger, const merger::MergeFileSystemPtr& mergeFileSystem,
                                            const std::string& segmentPath, const std::string& indexPath,
                                            const index_base::SegmentDataVector& segmentDataVec,
                                            const index_base::SegmentTopologyInfo& targetTopoInfo,
                                            int64_t estimateMemUse)
    : MergeWorkItem(mergeFileSystem)
    , mMerger(merger)
    , mSegmentPath(segmentPath)
    , mIndexPath(indexPath)
    , mSegmentDataVec(segmentDataVec)
    , mTargetTopoInfo(targetTopoInfo)
{
    if (estimateMemUse == -1) {
        mRequiredResource = DoEstimateMemoryUse();
    } else {
        mRequiredResource = estimateMemUse;
    }
}

template <class MergerPtr>
void KVMergeWorkItem<MergerPtr>::DoProcess()
{
    try {
        processForNormal();
        ReportMetrics();
    } catch (util::ExceptionBase& e) {
        IE_LOG(ERROR, "Exception throwed when merging %s : %s", GetIdentifier().c_str(), e.ToString().c_str());
        throw;
    }
}

template <class MergerPtr>
void KVMergeWorkItem<MergerPtr>::processForNormal()
{
    int64_t timeBeforeMerge = autil::TimeUtility::currentTimeInMicroSeconds();
    const file_system::DirectoryPtr& segmentDir = mMergeFileSystem->GetDirectory(mSegmentPath);
    const file_system::DirectoryPtr& indexDir = mMergeFileSystem->GetDirectory(mIndexPath);
    IE_LOG(INFO, "Begin merging segmentDir = %s", segmentDir->DebugString().c_str());
    mMerger->Merge(segmentDir, indexDir, mSegmentDataVec, mTargetTopoInfo);
    int64_t timeAfterMerge = autil::TimeUtility::currentTimeInMicroSeconds();
    float mergeUseTime = (timeAfterMerge - timeBeforeMerge) / 1000000.0;
    int64_t timeAfterCheckpoint = autil::TimeUtility::currentTimeInMicroSeconds();
    float checkpointUseTime = (timeAfterCheckpoint - timeAfterMerge) / 1000000.0;
    IE_LOG(INFO,
           "Finish merging %s, use [%.3f] seconds, cost [%.2f], "
           "checkpoint use [%.3f] seconds",
           segmentDir->DebugString().c_str(), mergeUseTime, GetCost(), checkpointUseTime);
}

template <class MergerPtr>
void KVMergeWorkItem<MergerPtr>::Destroy()
{
    mMerger.reset();
}

template <class MergerPtr>
int64_t KVMergeWorkItem<MergerPtr>::DoEstimateMemoryUse() const
{
    if (mMerger) {
        return mMerger->EstimateMemoryUse(mSegmentDataVec, mTargetTopoInfo);
    }
    return 0;
}

template <class MergerPtr>
void KVMergeWorkItem<MergerPtr>::SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics)
{
    mMerger->SetMergeItemMetrics(itemMetrics);
}
}} // namespace indexlib::merger

#endif //__INDEXLIB_KV_MERGE_WORK_ITEM_H
