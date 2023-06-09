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
#include "indexlib/index/kkv/on_disk_kkv_iterator.h"

#include "indexlib/index/kkv/on_disk_kkv_segment_iterator.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskKKVIterator);

OnDiskKKVIterator::OnDiskKKVIterator(const config::IndexPartitionSchemaPtr& schema,
                                     const config::KKVIndexConfigPtr& kkvConfig,
                                     const config::MergeIOConfig& mergeIOConfig)
    : mIOConfig(mergeIOConfig)
    , mKKVConfig(kkvConfig)
    , mSchema(schema)
    , mCurPkeyIterator(NULL)
    , mEstimatedPkCount(0)
    , mTotalProgress(0)
    , mCurrentProgress(0)
{
}

OnDiskKKVIterator::~OnDiskKKVIterator()
{
    mCurPkeySegIterInfo.clear();
    IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, mCurPkeyIterator);
    mCurPkeyIterator = NULL;
    while (!mHeap.empty()) {
        OnDiskKKVSegmentIterator* iter = mHeap.top();
        DELETE_AND_SET_NULL(iter);
        mHeap.pop();
    }
    for (size_t i = 0; i < mToDeleteSegIterVec.size(); i++) {
        DELETE_AND_SET_NULL(mToDeleteSegIterVec[i]);
    }
}

void OnDiskKKVIterator::Init(const vector<SegmentData>& segmentDatas)
{
    mTotalProgress = 0;
    for (size_t i = 0; i < segmentDatas.size(); i++) {
        if (IsEmptySegment(segmentDatas[i])) {
            continue;
        }

        OnDiskKKVSegmentIterator* segIter = new OnDiskKKVSegmentIterator(i);
        segIter->Init(mKKVConfig, segmentDatas[i]);
        segIter->ResetBufferParam(mIOConfig.readBufferSize, mIOConfig.enableAsyncRead);
        mSegIters.push_back(segIter);
        mTotalProgress += segIter->GetPkeyCount();
    }

    if (!CheckSortSequence(mSegIters)) {
        for (OnDiskKKVSegmentIterator* segIter : mSegIters) {
            DELETE_AND_SET_NULL(segIter);
        }
        INDEXLIB_FATAL_ERROR(UnSupported, "check sort sequence fail!");
    }

    mEstimatedPkCount = DoEstimatePKeyCount(mSegIters);

    for (size_t i = 0; i < mSegIters.size(); i++) {
        mSegIters[i]->Reset();
        PushBackToHeap(mSegIters[i]);
    }
    MoveToNext();
}

bool OnDiskKKVIterator::CheckSortSequence(const vector<OnDiskKKVSegmentIterator*>& segIters)
{
    size_t sortSegmentCount = 0;
    for (size_t i = 0; i < segIters.size(); i++) {
        if (segIters[i]->KeepSortSequence()) {
            sortSegmentCount++;
        }
    }

    if (sortSegmentCount == 0) {
        return true;
    }
    if (sortSegmentCount == 1 && segIters.size() == 1) {
        return true;
    }

    IE_LOG(ERROR, "not support mix more than one sort segment & unsort segment");
    return false;
}

size_t OnDiskKKVIterator::DoEstimatePKeyCount(const vector<OnDiskKKVSegmentIterator*>& segIters)
{
    bool mergeUsePreciseCount = mKKVConfig->GetIndexPreference().GetHashDictParam().UsePreciseCountWhenMerge();
    size_t count = 0;
    if (!mergeUsePreciseCount) {
        for (size_t i = 0; i < segIters.size(); i++) {
            count += segIters[i]->GetPkeyCount();
        }
        return count;
    }

    SegmentIterHeap heap;
    for (size_t i = 0; i < segIters.size(); i++) {
        segIters[i]->Reset();
        if (segIters[i]->IsValid()) {
            heap.push(segIters[i]);
        }
    }

    uint64_t lastPkey = 0;
    bool isFirstKey = true;
    while (!heap.empty()) {
        OnDiskKKVSegmentIterator* segIter = heap.top();
        heap.pop();
        regionid_t regionId;
        uint64_t curPkey = segIter->GetPrefixKey(regionId);
        segIter->MoveToNext();
        if (segIter->IsValid()) {
            heap.push(segIter);
        }

        if (isFirstKey) {
            count++;
            lastPkey = curPkey;
            isFirstKey = false;
            continue;
        }

        if (curPkey != lastPkey) {
            count++;
            lastPkey = curPkey;
        }
    }
    return count;
}

void OnDiskKKVIterator::MoveToNext()
{
    mCurPkeySegIterInfo.clear();
    IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, mCurPkeyIterator);
    mCurPkeyIterator = NULL;
    if (mPool.getUsedBytes() >= RESET_POOL_THRESHOLD) {
        mPool.reset();
    }
    CreateCurrentPkeyIterator();
}

void OnDiskKKVIterator::CreateCurrentPkeyIterator()
{
    assert(mCurPkeySegIterInfo.empty());
    assert(mCurPkeyIterator == NULL);

    bool isFirstKey = true;
    uint64_t firstPkey = 0;
    regionid_t firstRegionId = INVALID_REGIONID;
    bool isPkeyDeleted = false;
    uint32_t deletePkeyTs = 0;
    while (!mHeap.empty()) {
        ++mCurrentProgress;
        OnDiskKKVSegmentIterator* segIter = mHeap.top();
        regionid_t currentRegionId;
        uint64_t curPkey = segIter->GetPrefixKey(currentRegionId);
        auto regionKkvConfig = DYNAMIC_POINTER_CAST(
            config::KKVIndexConfig,
            mSchema->GetRegionSchema(currentRegionId)->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        assert(regionKkvConfig);
        if (isFirstKey) {
            firstPkey = curPkey;
            firstRegionId = currentRegionId;
        } else if (curPkey != firstPkey) // different pkey
        {
            break;
        } else if (currentRegionId != firstRegionId) // same pkey with different RegionId
        {
            IE_LOG(ERROR, "Hash collision occur for hashKey(%lu), with different regionid(%d, %d)", curPkey,
                   currentRegionId, firstRegionId);
        }
        mHeap.pop();
        assert(segIter->IsValid());
        if (!isPkeyDeleted) {
            auto singlePkeyIter = segIter->GetIterator(&mPool, regionKkvConfig);
            isPkeyDeleted = singlePkeyIter->HasPKeyDeleted();
            deletePkeyTs = singlePkeyIter->GetPKeyDeletedTs();
            if (singlePkeyIter->IsValid() && currentRegionId == firstRegionId) {
                mCurPkeySegIterInfo.push_back(make_pair(segIter->GetSegmentIdx(), singlePkeyIter));
            } else {
                // delete pkey or region id mismatch
                IE_POOL_COMPATIBLE_DELETE_CLASS(&mPool, singlePkeyIter);
            }
        }

        segIter->MoveToNext();
        PushBackToHeap(segIter);
        isFirstKey = false;
    }

    if (mCurPkeySegIterInfo.empty() && !isPkeyDeleted) {
        return;
    }
    mCurPkeyIterator = POOL_COMPATIBLE_NEW_CLASS((&mPool), OnDiskSinglePKeyIterator, (&mPool), firstPkey, isPkeyDeleted,
                                                 deletePkeyTs, firstRegionId);
    mCurPkeyIterator->Init(mCurPkeySegIterInfo);
}

bool OnDiskKKVIterator::IsEmptySegment(const SegmentData& segData)
{
    DirectoryPtr dataDir = segData.GetIndexDirectory(mKKVConfig->GetIndexName(), false);
    if (!dataDir) {
        IE_LOG(INFO, "segment [%u], shardingIdx [%u] is empty!", segData.GetSegmentId(), segData.GetShardId());
        return true;
    }
    return !dataDir->IsExist(PREFIX_KEY_FILE_NAME);
}

void OnDiskKKVIterator::PushBackToHeap(OnDiskKKVSegmentIterator* segIter)
{
    if (segIter->IsValid()) {
        mHeap.push(segIter);
    } else {
        mToDeleteSegIterVec.push_back(segIter);
    }
}

double OnDiskKKVIterator::GetMergeProgressRatio() const
{
    return mCurrentProgress < mTotalProgress ? (double)(mCurrentProgress) / mTotalProgress : 1.0f;
}
}} // namespace indexlib::index
