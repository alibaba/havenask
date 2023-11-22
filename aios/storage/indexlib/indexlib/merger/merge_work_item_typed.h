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

#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "indexlib/common_define.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_work_item.h"
#include "indexlib/merger/segment_directory.h"

namespace indexlib { namespace merger {

template <class MergerPtr, class MergerResource>
class MergeWorkItemTyped : public MergeWorkItem
{
public:
    MergeWorkItemTyped(MergerPtr& merger, const SegmentDirectoryPtr& segDir, const MergerResource& resource,
                       const index_base::SegmentMergeInfos& segMergeInfos,
                       const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                       const merger::MergeFileSystemPtr& mergeFileSystem, bool isSortedMerge);
    ~MergeWorkItemTyped() {}

public:
    void DoProcess() override;
    void Destroy() override;
    int64_t GetRequiredResource() const override { return mRequiredResource; }
    int64_t EstimateMemoryUse() const override { return DoEstimateMemoryUse(); }

    void SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics) override;

public:
    const std::string& TEST_GetPath() const { return mOutputSegmentMergeInfos[0].path; }

private:
    void processForNormal();
    void processForSorted();
    int64_t DoEstimateMemoryUse() const;

private:
    MergerPtr mMerger;
    SegmentDirectoryPtr mSegmentDirectory;
    MergerResource mResource;
    index_base::SegmentMergeInfos mSegmentMergeInfos;
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;
    bool mIsSortedMerge;
    int64_t mRequiredResource;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE2(index, MergeWorkItemTyped);

template <class MergerPtr, class MergerResource>
MergeWorkItemTyped<MergerPtr, MergerResource>::MergeWorkItemTyped(
    MergerPtr& merger, const SegmentDirectoryPtr& segDir, const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos, const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
    const merger::MergeFileSystemPtr& mergeFileSystem, bool isSortedMerge)
    : MergeWorkItem(mergeFileSystem)
    , mMerger(merger)
    , mSegmentDirectory(segDir)
    , mResource(resource)
    , mSegmentMergeInfos(segMergeInfos)
    , mOutputSegmentMergeInfos(outputSegMergeInfos)
    , mIsSortedMerge(isSortedMerge)
{
    mRequiredResource = DoEstimateMemoryUse();
}

template <class MergerPtr, class MergerResource>
void MergeWorkItemTyped<MergerPtr, MergerResource>::DoProcess()
{
    for (auto& outputInfo : mOutputSegmentMergeInfos) {
        outputInfo.directory = mMergeFileSystem->GetDirectory(outputInfo.path);
    }
    try {
        if (mIsSortedMerge) {
            processForSorted();
        } else {
            processForNormal();
        }
        ReportMetrics();
    } catch (util::ExceptionBase& e) {
        IE_LOG(ERROR, "Exception throwed when merging %s : %s", GetIdentifier().c_str(), e.ToString().c_str());
        beeper::EventTags tags;
        BEEPER_FORMAT_REPORT("bs_worker_error", tags, "Exception throwed when merging %s : %s", GetIdentifier().c_str(),
                             e.ToString().c_str());
        throw;
    }
}

template <class MergerPtr, class MergerResource>
void MergeWorkItemTyped<MergerPtr, MergerResource>::processForNormal()
{
    int64_t timeBeforeMerge = autil::TimeUtility::currentTimeInMicroSeconds();
    IE_LOG(INFO, "Begin merging %s", GetIdentifier().c_str());
    mMerger->BeginMerge(mSegmentDirectory);

    mMerger->Merge(mResource, mSegmentMergeInfos, mOutputSegmentMergeInfos);
    int64_t timeAfterMerge = autil::TimeUtility::currentTimeInMicroSeconds();
    float useTime = (timeAfterMerge - timeBeforeMerge) / 1000000.0;
    IE_LOG(INFO, "Finish merging %s, use [%.3f] seconds, cost [%.2f]", GetIdentifier().c_str(), useTime, GetCost());
    if (useTime > (float)900) {
        beeper::EventTags tags;
        BEEPER_FORMAT_REPORT("bs_worker_error", tags, "merge [%s] [cost: %.2f] use [%.3f] seconds, over 15 min.",
                             GetIdentifier().c_str(), GetCost(), useTime);
    }

    if (mCounter) {
        mCounter->Set(useTime);
    }
}

template <class MergerPtr, class MergerResource>
void MergeWorkItemTyped<MergerPtr, MergerResource>::processForSorted()
{
    int64_t timeBeforeMerge = autil::TimeUtility::currentTimeInMicroSeconds();
    IE_LOG(INFO, "Begin sort-by-weight merging %s", GetIdentifier().c_str());
    mMerger->BeginMerge(mSegmentDirectory);
    mMerger->SortByWeightMerge(mResource, mSegmentMergeInfos, mOutputSegmentMergeInfos);
    int64_t timeAfterMerge = autil::TimeUtility::currentTimeInMicroSeconds();
    float useTime = (timeAfterMerge - timeBeforeMerge) / 1000000.0;
    IE_LOG(INFO, "Finish sort-by-weight merging %s, use [%.3f] seconds, cost [%.2f]", GetIdentifier().c_str(), useTime,
           GetCost());
    if (useTime > (float)900) {
        beeper::EventTags tags;
        BEEPER_FORMAT_REPORT("bs_worker_error", tags,
                             "sort-by-weigth merge [%s] [cost: %.2f] use [%.3f] seconds, over 15 min.",
                             GetIdentifier().c_str(), GetCost(), useTime);
    }

    if (mCounter) {
        mCounter->Set(useTime);
    }
}

template <class MergerPtr, class MergerResource>
void MergeWorkItemTyped<MergerPtr, MergerResource>::Destroy()
{
    mMerger.reset();
}

template <class MergerPtr, class MergerResource>
int64_t MergeWorkItemTyped<MergerPtr, MergerResource>::DoEstimateMemoryUse() const
{
    if (mMerger) {
        return mMerger->EstimateMemoryUse(mSegmentDirectory, mResource, mSegmentMergeInfos, mOutputSegmentMergeInfos,
                                          mIsSortedMerge);
    }
    return 0;
}

template <class MergerPtr, class MergerResource>
void MergeWorkItemTyped<MergerPtr, MergerResource>::SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics)
{
    // TODO: support
    // mMerger->SetMergeItemMetrics(itemMetrics);
}
}} // namespace indexlib::merger
