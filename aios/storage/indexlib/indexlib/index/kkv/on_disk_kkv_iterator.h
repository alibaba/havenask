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
#include <queue>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/kkv/on_disk_kkv_segment_iterator.h"
#include "indexlib/index/kkv/on_disk_single_pkey_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class OnDiskKKVIterator
{
public:
    static const size_t RESET_POOL_THRESHOLD = 20 * 1024 * 1024; // 20 Mbytes

public:
    OnDiskKKVIterator(const config::IndexPartitionSchemaPtr& schema, const config::KKVIndexConfigPtr& kkvConfig,
                      const config::MergeIOConfig& mergeIoConfig = config::MergeIOConfig());
    ~OnDiskKKVIterator();

public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas);
    bool IsEmpty() { return mHeap.empty(); }

    void MoveToNext();
    bool IsValid() const { return mCurPkeyIterator != NULL; }
    OnDiskSinglePKeyIterator* GetCurrentIterator() const { return mCurPkeyIterator; }
    size_t EstimatePkeyCount() const { return mEstimatedPkCount; }

    double GetMergeProgressRatio() const;

    size_t GetPkeyCount(size_t segmentIndex) { return mSegIters[segmentIndex]->GetPkeyCount(); }

private:
    bool IsEmptySegment(const index_base::SegmentData& segData);
    void CreateCurrentPkeyIterator();
    void PushBackToHeap(OnDiskKKVSegmentIterator* iter);
    size_t DoEstimatePKeyCount(const std::vector<OnDiskKKVSegmentIterator*>& segIters);
    bool CheckSortSequence(const std::vector<OnDiskKKVSegmentIterator*>& segIters);

private:
    struct SegIterComparator {
        bool operator()(OnDiskKKVSegmentIterator*& lft, OnDiskKKVSegmentIterator*& rht)
        {
            regionid_t leftRegionId = INVALID_REGIONID;
            regionid_t rightRegionId = INVALID_REGIONID;
            uint64_t lpkey = lft->GetPrefixKey(leftRegionId);
            uint64_t rpkey = rht->GetPrefixKey(rightRegionId);
            if (lpkey == rpkey) {
                return lft->GetSegmentIdx() < rht->GetSegmentIdx();
            }
            return lpkey > rpkey;
        }
    };

    typedef std::priority_queue<OnDiskKKVSegmentIterator*, std::vector<OnDiskKKVSegmentIterator*>, SegIterComparator>
        SegmentIterHeap;

private:
    autil::mem_pool::Pool mPool;
    config::MergeIOConfig mIOConfig;
    config::KKVIndexConfigPtr mKKVConfig;
    config::IndexPartitionSchemaPtr mSchema;

    SegmentIterHeap mHeap;
    std::vector<OnDiskKKVSegmentIterator*> mSegIters;
    std::vector<OnDiskKKVSegmentIterator*> mToDeleteSegIterVec;
    OnDiskSinglePKeyIterator* mCurPkeyIterator;
    OnDiskSinglePKeyIterator::MultiSegmentSinglePKeyIterInfo mCurPkeySegIterInfo;

    size_t mEstimatedPkCount;
    size_t mTotalProgress;
    size_t mCurrentProgress;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskKKVIterator);
}} // namespace indexlib::index
