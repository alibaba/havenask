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

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/on_disk_kkv_segment_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class OnDiskSinglePKeyIterator : public KKVSegmentIterator
{
public:
    typedef OnDiskKKVSegmentIterator::SinglePKeyIterator SingleSegmentSinglePKeyIterator;
    // first: segmentIdx
    typedef std::pair<size_t, SingleSegmentSinglePKeyIterator*> SingleSegmentSinglePKeyIterInfo;
    typedef std::vector<SingleSegmentSinglePKeyIterInfo> MultiSegmentSinglePKeyIterInfo;

public:
    OnDiskSinglePKeyIterator(autil::mem_pool::Pool* sessionPool, uint64_t pkeyHash, bool isPkeyDelete,
                             uint32_t deletePkTs, regionid_t regionId)
        : KKVSegmentIterator(sessionPool)
        , mPKeyHash(pkeyHash)
        , mIsPkeyDeleted(isPkeyDelete)
        , mDeletePkeyTs(deletePkTs)
        , mRegionId(regionId)
    {
    }

    ~OnDiskSinglePKeyIterator()
    {
        while (!mHeap.empty()) {
            SingleSegmentSinglePKeyIterInfo singleSegIterInfo = mHeap.top();
            mHeap.pop();
            POOL_COMPATIBLE_DELETE_CLASS(mPool, singleSegIterInfo.second);
        }
    }

public:
    void Init(const std::vector<SingleSegmentSinglePKeyIterInfo>& multiSegIterInfo);
    const std::vector<size_t>& GetSegementIdxs() const { return mSegmentIdxs; }

    bool IsValid() const override { return !mHeap.empty(); }
    bool HasPKeyDeleted() const override { return mIsPkeyDeleted; }
    uint32_t GetPKeyDeletedTs() const override { return mDeletePkeyTs; }

    bool MoveToNext() override;
    void GetCurrentSkey(uint64_t& skey, bool& isDeleted) const override;
    void GetCurrentValue(autil::StringView& value) override;
    uint32_t GetCurrentTs() override;
    uint32_t GetCurrentExpireTime() override;
    bool CurrentSKeyExpired(uint64_t currentTsInSecond) override;
    size_t GetCurrentSegmentIdx();

    int Compare(const KKVSegmentIterator* other) override final
    {
        assert(false); // should not be called
        assert(other);
        uint64_t leftSkey, rightSkey;
        bool isDeleted;
        GetCurrentSkey(leftSkey, isDeleted);
        other->GetCurrentSkey(rightSkey, isDeleted);
        if (leftSkey == rightSkey) {
            return 0;
        }
        return leftSkey < rightSkey ? -1 : 1;
    }

    uint64_t GetPKeyHash() const { return mPKeyHash; }
    regionid_t GetRegionId() const { return mRegionId; }

private:
    struct SegIterComparator {
        bool operator()(SingleSegmentSinglePKeyIterInfo& lft, SingleSegmentSinglePKeyIterInfo& rht)
        {
            assert(lft.second->IsValid());
            assert(rht.second->IsValid());
            int skeyCompRet = lft.second->Compare(rht.second);
            if (skeyCompRet == 0) {
                return lft.first < rht.first;
            }
            return skeyCompRet > 0;
        }
    };

    typedef std::priority_queue<SingleSegmentSinglePKeyIterInfo, std::vector<SingleSegmentSinglePKeyIterInfo>,
                                SegIterComparator>
        SegmentIterHeap;

private:
    SegmentIterHeap mHeap;
    uint64_t mPKeyHash;
    bool mIsPkeyDeleted;
    uint32_t mDeletePkeyTs;
    regionid_t mRegionId;
    std::vector<size_t> mSegmentIdxs;
    std::vector<SingleSegmentSinglePKeyIterInfo> mSegIterInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskSinglePKeyIterator);
}} // namespace indexlib::index
