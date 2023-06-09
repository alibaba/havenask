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

#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/merge/OnDiskKKVSegmentIterator.h"

namespace indexlibv2::index {

template <typename SKeyType>
class OnDiskSinglePKeyIterator final : public KKVSegmentIteratorBase<SKeyType>
{
public:
    using Base = KKVSegmentIteratorBase<SKeyType>;
    using SingleSegmentSinglePKeyIterator = typename OnDiskKKVSegmentIterator<SKeyType>::SinglePKeyIterator;
    // first: segmentIdx
    using SingleSegmentSinglePKeyIterInfo = std::pair<size_t, SingleSegmentSinglePKeyIterator*>;
    using MultiSegmentSinglePKeyIterInfo = std::vector<SingleSegmentSinglePKeyIterInfo>;
    using Base::_expireTime;
    using Base::_hasPKeyDeleted;
    using Base::_isValid;
    using Base::_pkeyDeletedTs;
    using Base::_skey;
    using Base::_skeyDeleted;
    using Base::_timestamp;

public:
    OnDiskSinglePKeyIterator(autil::mem_pool::Pool* sessionPool, const config::KKVIndexConfig* indexConfig,
                             uint64_t pkeyHash, bool isPkeyDelete, uint32_t deletePkTs)
        : Base(sessionPool, indexConfig)
        , _pkeyHash(pkeyHash)
    {
        _hasPKeyDeleted = isPkeyDelete;
        _pkeyDeletedTs = deletePkTs;
    }

    ~OnDiskSinglePKeyIterator()
    {
        while (!_heap.empty()) {
            SingleSegmentSinglePKeyIterInfo singleSegIterInfo = _heap.top();
            _heap.pop();
            POOL_COMPATIBLE_DELETE_CLASS(Base::_pool, singleSegIterInfo.second);
        }
    }

public:
    void Init(const std::vector<SingleSegmentSinglePKeyIterInfo>& multiSegIterInfo);
    const std::vector<size_t>& GetSegementIdxs() const { return _segmentIdxs; }

    void MoveToNext() override;
    void GetCurrentValue(autil::StringView& value) override;

public:
    bool CurrentSKeyExpired(uint64_t currentTsInSecond);
    size_t GetCurrentSegmentIdx();
    uint64_t GetPKeyHash() const { return _pkeyHash; }

private:
    void BatchGet(SKeySearchContext<SKeyType>* skeyContext, uint64_t minTsInSecond, uint64_t curTsInSecond,
                  typename SKeySearchContext<SKeyType>::PooledSKeySet& foundSKeys,
                  KKVResultBuffer& resultBuffer) override
    {
        assert(false);
    }

private:
    struct SegIterComparator {
        bool operator()(SingleSegmentSinglePKeyIterInfo& lft, SingleSegmentSinglePKeyIterInfo& rht)
        {
            assert(lft.second->IsValid());
            assert(rht.second->IsValid());
            SKeyType leftSKey = lft.second->GetCurrentSkey();
            SKeyType rightSKey = rht.second->GetCurrentSkey();
            if (leftSKey == rightSKey) {
                return lft.first < rht.first;
            }
            return leftSKey > rightSKey;
        }
    };

    using SegmentIterHeap = std::priority_queue<SingleSegmentSinglePKeyIterInfo,
                                                std::vector<SingleSegmentSinglePKeyIterInfo>, SegIterComparator>;

    void FillSKeyInfo();

private:
    SegmentIterHeap _heap;
    uint64_t _pkeyHash;
    std::vector<size_t> _segmentIdxs;
    std::vector<SingleSegmentSinglePKeyIterInfo> _segIterInfos;

private:
    AUTIL_LOG_DECLARE();
};

MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(OnDiskSinglePKeyIterator);

} // namespace indexlibv2::index
