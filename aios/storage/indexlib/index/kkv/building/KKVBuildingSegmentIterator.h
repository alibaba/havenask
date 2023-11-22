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

#include "autil/MultiValueType.h"
#include "indexlib/index/kkv/building/BuildingSKeyIterator.h"
#include "indexlib/index/kkv/building/KKVValueWriter.h"
#include "indexlib/index/kkv/common/KKVResultBuffer.h"
#include "indexlib/index/kkv/common/KKVSegmentIteratorBase.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVBuildingSegmentIterator final : public KKVSegmentIteratorBase<SKeyType>
{
private:
    using Base = KKVSegmentIteratorBase<SKeyType>;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    using PooledSKeyVector = typename SKeyContext::PooledSKeyVector;
    using Base::_expireTime;
    using Base::_hasPKeyDeleted;
    using Base::_isValid;
    using Base::_pkeyDeletedTs;
    using Base::_skey;
    using Base::_skeyDeleted;
    using Base::_timestamp;

public:
    KKVBuildingSegmentIterator(const std::shared_ptr<SKeyWriter<SKeyType>>& skeyWriter,
                               const SKeyListInfo& skeyListInfo, const std::shared_ptr<KKVValueWriter>& valueWriter,
                               autil::mem_pool::Pool* sessionPool, const config::KKVIndexConfig* indexConfig)
        : Base(sessionPool, indexConfig)
        , _skeyIterator(skeyWriter, skeyListInfo)
        , _valueWriter(valueWriter)
        , _storeExpireTime(indexConfig->StoreExpireTime())
    {
        auto& valueConfig = Base::_indexConfig->GetValueConfig();
        _fixedValueLen = valueConfig->GetFixedLength();

        _hasPKeyDeleted = _skeyIterator.HasPKeyDeleted();
        if (_hasPKeyDeleted) {
            _pkeyDeletedTs = _skeyIterator.GetPKeyDeletedTs();
        }
        FillSKeyInfo();
    }
    ~KKVBuildingSegmentIterator() = default;

public:
    void MoveToNext() override
    {
        _skeyIterator.MoveToNext();
        FillSKeyInfo();
    }
    void GetCurrentValue(autil::StringView& value) override { DoGetValue(value, _skeyIterator.GetValueOffset()); }
    void BatchGet(SKeyContext* skeyContext, uint64_t minTsInSecond, uint64_t curTsInSecond, PooledSKeySet& foundSKeys,
                  KKVResultBuffer& resultBuffer) override;

private:
    bool ValidateSKey(SKeyContext* skeyContext, uint64_t minTsInSecond, uint64_t curTsInSecond,
                      PooledSKeySet& foundSKeys);
    void FillSKeyInfo()
    {
        _isValid = _skeyIterator.IsValid();
        if (_isValid) {
            _skey = _skeyIterator.GetSKey();
            _skeyDeleted = _skeyIterator.IsDeleted();
            _timestamp = _skeyIterator.GetTs();
            _expireTime = _skeyIterator.GetExpireTime();
        }
    }

    void DoGetValue(autil::StringView& value, size_t offset) const
    {
        const char* valueAddr = _valueWriter->GetValue(offset);
        if (_fixedValueLen > 0) {
            value = autil::StringView(valueAddr, _fixedValueLen);
        } else {
            autil::MultiChar mc;
            mc.init(valueAddr);
            value = autil::StringView(mc.data(), mc.size());
        }
    }

    bool MoveToRequiredSKey(const PooledSKeyVector& sortedRequiredSKeys)
    {
        while (_skeyIterator.IsValid() && _requiredSKeyIdx < sortedRequiredSKeys.size()) {
            // 迭代到第一个不小于skey的位置。若正好等于skey,返回true
            SKeyType skey = sortedRequiredSKeys[_requiredSKeyIdx++];
            if (_skeyIterator.MoveToSKey(skey)) {
                FillSKeyInfo();
                return true;
            }
        }
        return false;
    }

private:
    BuildingSKeyIterator<SKeyType> _skeyIterator;
    const std::shared_ptr<KKVValueWriter> _valueWriter;
    bool _storeExpireTime = false;
    int32_t _fixedValueLen = -1;
    uint32_t _requiredSKeyIdx = 0u;
};

template <typename SKeyType>
void KKVBuildingSegmentIterator<SKeyType>::BatchGet(SKeyContext* skeyContext, uint64_t minTsInSecond,
                                                    uint64_t curTsInSecond, PooledSKeySet& foundSKeys,
                                                    KKVResultBuffer& resultBuffer)
{
    size_t begin = resultBuffer.Size();
    for (; Base::IsValid(); MoveToNext()) {
        if (skeyContext) {
            const auto& requiredSKeys = skeyContext->GetSortedRequiredSKeys();
            if (foundSKeys.size() >= requiredSKeys.size() || !MoveToRequiredSKey(requiredSKeys)) {
                _isValid = false;
                break;
            }
        }
        if (!ValidateSKey(skeyContext, minTsInSecond, curTsInSecond, foundSKeys)) {
            continue;
        }
        resultBuffer.EmplaceBack(KKVDoc(_skey, _timestamp, _skeyIterator.GetValueOffset()));
        if (resultBuffer.ReachLimit()) {
            _isValid = false;
            break;
        }
        if (resultBuffer.IsFull()) {
            // buffer满，再次遍历该segment时，从下个位置开始
            MoveToNext();
            break;
        }
    }
    uint32_t end = resultBuffer.Size();
    for (size_t i = begin; i < end; i++) {
        DoGetValue(resultBuffer[i].value, resultBuffer[i].offset);
    }
}

template <typename SKeyType>
bool KKVBuildingSegmentIterator<SKeyType>::ValidateSKey(SKeyContext* skeyContext, uint64_t minTsInSecond,
                                                        uint64_t curTsInSecond, PooledSKeySet& foundSKeys)
{
    if (_timestamp < minTsInSecond) {
        return false;
    }

    if (!Base::_isOnlySeg) {
        if (Base::_isLastSeg && !skeyContext) {
            if (foundSKeys.find(_skey) != foundSKeys.end()) {
                return false;
            }
        } else {
            if (!foundSKeys.insert(_skey).second) {
                return false;
            }
        }
    }

    if (_skeyDeleted) {
        return false;
    }

    if (_storeExpireTime && Base::SKeyExpired(curTsInSecond, _timestamp, _expireTime)) {
        return false;
    }
    return true;
}

} // namespace indexlibv2::index
