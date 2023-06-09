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

#include "indexlib/index/kkv/built/BuiltSKeyIterator.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIteratorBase.h"
#include "indexlib/index/kkv/built/KKVBuiltValueReader.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlibv2::index {

template <typename SKeyType, typename Option>
class KKVBuiltSegmentIterator final : public KKVBuiltSegmentIteratorBase<SKeyType>
{
protected:
    static constexpr bool valueInline = Option::valueInline;
    static constexpr bool storeTs = Option::storeTs;
    static constexpr bool storeExpireTime = Option::storeExpireTime;
    using Base = KKVSegmentIteratorBase<SKeyType>;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    using SKeyIterator = BuiltSKeyIterator<SKeyType, Option>;
    using Base::_expireTime;
    using Base::_hasPKeyDeleted;
    using Base::_isValid;
    using Base::_pkeyDeletedTs;
    using Base::_skey;
    using Base::_skeyDeleted;
    using Base::_timestamp;

    enum SKeyStatus { INVALID = 0, VALID, FILTERED };

public:
    KKVBuiltSegmentIterator(const config::KKVIndexConfig* indexConfig, bool keepSortSeq, uint32_t defaultTs,
                            bool isOnline, autil::mem_pool::Pool* pool, KKVMetricsCollector* metricsCollector)
        : KKVBuiltSegmentIteratorBase<SKeyType>(pool, indexConfig, keepSortSeq)
        , _isOnline(isOnline)
        , _skeyIterator(defaultTs, isOnline, pool, indexConfig->GetValueConfig()->GetFixedLength(),
                        CreateReadOption(metricsCollector))
        , _metricsCollector(metricsCollector)
    {
    }
    ~KKVBuiltSegmentIterator()
    {
        if (_metricsCollector != nullptr) {
            _metricsCollector->IncSKeyChunkCountInBlocks(_skeyIterator.GetSKeyChunkCount());
        }
    }

public:
    FL_LAZY(Status)
    Init(indexlib::file_system::FileReader* skeyFileReader, indexlib::file_system::FileReader* valueFileReader,
         OnDiskPKeyOffset firstSkeyOffset);
    void BatchGet(SKeyContext* skeyContext, uint64_t minTsInSecond, uint64_t curTsInSecond, PooledSKeySet& foundSKeys,
                  KKVResultBuffer& resultBuffer) override;
    void MoveToNext() override
    {
        _skeyIterator.MoveToNext();
        FillSKeyInfo();
    }
    void GetCurrentValue(autil::StringView& value) override
    {
        if constexpr (valueInline) {
            value = _skeyIterator.GetValue();
        } else {
            value = _valueReader->Read(_skeyIterator.GetValueOffset());
        }
    }

public:
    FL_LAZY(bool)
    GetSKeysAsync(SKeySearchContext<SKeyType>* skeyContext, uint64_t minTsInSecond, KKVDocs& result,
                  KKVBuiltValueFetcher& valueFetcher) override;

private:
    SKeyStatus ValidateSKey(SKeyContext* skeyContext, uint64_t minTsInSecond, uint64_t curTsInSecond,
                            PooledSKeySet& foundSKeys);
    void FillSKeyInfo();
    indexlib::file_system::ReadOption CreateReadOption(KVMetricsCollector* metricsCollector);

private:
    bool _isOnline = false;
    SKeyIterator _skeyIterator;
    indexlib::util::PooledUniquePtr<KKVBuiltValueReader> _valueReader;
    KVMetricsCollector* _metricsCollector = nullptr;
};

template <typename SKeyType, typename Option>
FL_LAZY(Status)
KKVBuiltSegmentIterator<SKeyType, Option>::Init(indexlib::file_system::FileReader* skeyFileReader,
                                                indexlib::file_system::FileReader* valueFileReader,
                                                OnDiskPKeyOffset firstSkeyOffset)
{
    assert(skeyFileReader != nullptr);
    const auto& skeyParam = Base::_indexConfig->GetIndexPreference().GetSkeyParam();
    auto status = FL_COAWAIT _skeyIterator.Init(skeyFileReader, skeyParam.EnableFileCompress(), firstSkeyOffset);
    if (!status.IsOK()) {
        FL_CORETURN status;
    }

    if constexpr (!valueInline) {
        assert(valueFileReader != nullptr);
        _valueReader = indexlib::util::MakePooledUniquePtr<KKVBuiltValueReader>(
            Base::_pool, Base::_indexConfig->GetValueConfig()->GetFixedLength(), _isOnline, Base::_pool,
            CreateReadOption(_metricsCollector));
        const auto& valueParam = Base::_indexConfig->GetIndexPreference().GetValueParam();
        _valueReader->Init(valueFileReader, valueParam.EnableFileCompress());
    }

    _hasPKeyDeleted = _skeyIterator.HasPKeyDeleted();
    if (_hasPKeyDeleted) {
        _pkeyDeletedTs = _skeyIterator.GetPKeyDeletedTs();
    }
    FillSKeyInfo();
    FL_CORETURN Status::OK();
}

template <typename SKeyType, typename Option>
void KKVBuiltSegmentIterator<SKeyType, Option>::BatchGet(SKeyContext* skeyContext, uint64_t minTsInSecond,
                                                         uint64_t curTsInSecond, PooledSKeySet& foundSKeys,
                                                         KKVResultBuffer& resultBuffer)
{
    size_t begin = resultBuffer.Size();
    for (; Base::IsValid(); MoveToNext()) {
        auto status = ValidateSKey(skeyContext, minTsInSecond, curTsInSecond, foundSKeys);
        if (status == SKeyStatus::INVALID) {
            _isValid = false;
            break;
        } else if (status == SKeyStatus::FILTERED) {
            continue;
        }

        if constexpr (valueInline) {
            resultBuffer.EmplaceBack(KKVDoc(_skey, _timestamp, _skeyIterator.GetValue()));
        } else {
            resultBuffer.EmplaceBack(KKVDoc(_skey, _timestamp, _skeyIterator.GetValueOffset()));
        }
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

    if constexpr (!valueInline) {
        assert(_valueReader != nullptr);
        size_t end = resultBuffer.Size();
        for (size_t i = begin; i < end; ++i) {
            resultBuffer[i].value = _valueReader->Read(resultBuffer[i].valueOffset);
        }
    }
}

template <typename SKeyType, typename Option>
typename KKVBuiltSegmentIterator<SKeyType, Option>::SKeyStatus
KKVBuiltSegmentIterator<SKeyType, Option>::ValidateSKey(SKeyContext* skeyContext, uint64_t minTsInSecond,
                                                        uint64_t curTsInSecond, PooledSKeySet& foundSKeys)
{
    if (_timestamp < minTsInSecond) {
        return SKeyStatus::FILTERED;
    }

    if (skeyContext) {
        const auto& requiredSKeys = skeyContext->GetRequiredSKeys();
        if (requiredSKeys.size() == foundSKeys.size() ||
            (!Base::_keepSortSeq && _skey > skeyContext->GetMaxRequiredSKey())) {
            return SKeyStatus::INVALID;
        }
        if (requiredSKeys.find(_skey) == requiredSKeys.end()) {
            return SKeyStatus::FILTERED;
        }
    }

    if (!Base::_isOnlySeg) {
        if (Base::_isLastSeg && !skeyContext) {
            // last segment only find, not insert
            if (foundSKeys.find(_skey) != foundSKeys.end()) {
                return SKeyStatus::FILTERED;
            }
        } else {
            if (!foundSKeys.insert(_skey).second) {
                return SKeyStatus::FILTERED;
            }
        }
    }

    if (_skeyDeleted) {
        return SKeyStatus::FILTERED;
    }
    if constexpr (storeExpireTime) {
        if (Base::SKeyExpired(curTsInSecond, _timestamp, _expireTime)) {
            return SKeyStatus::FILTERED;
        }
    }
    return SKeyStatus::VALID;
}

template <typename SKeyType, typename Option>
FL_LAZY(bool)
KKVBuiltSegmentIterator<SKeyType, Option>::GetSKeysAsync(SKeySearchContext<SKeyType>* skeyContext,
                                                         uint64_t minTsInSecond, KKVDocs& result,
                                                         KKVBuiltValueFetcher& valueFetcher)
{
    while (Base::IsValid()) {
        // coroutine search need deleted skey
        if (_timestamp >= minTsInSecond && (!skeyContext || skeyContext->MatchRequiredSKey(_skey))) {
            if constexpr (valueInline) {
                result.push_back(KKVDoc(_skey, _timestamp, _expireTime, _skeyDeleted, _skeyIterator.GetValue()));
            } else {
                result.push_back(KKVDoc(_skey, _timestamp, _expireTime, _skeyDeleted, _skeyIterator.GetValueOffset()));
            }
        }
        if (_skeyIterator.NeedSwitchChunk()) {
            if (!(FL_COAWAIT _skeyIterator.SwitchChunk())) {
                FL_CORETURN false;
            }
        }
        _skeyIterator.MoveToNextInChunk();
        FillSKeyInfo();
    }
    if constexpr (!valueInline) {
        valueFetcher.Init(_valueReader.get());
    }
    FL_CORETURN true;
}

template <typename SKeyType, typename Option>
void KKVBuiltSegmentIterator<SKeyType, Option>::FillSKeyInfo()
{
    _isValid = _skeyIterator.IsValid();
    if (_isValid) {
        _skey = _skeyIterator.GetSKey();
        _skeyDeleted = _skeyIterator.IsDeleted();
        _timestamp = _skeyIterator.GetTs();
        _expireTime = _skeyIterator.GetExpireTime();
    }
}

template <typename SKeyType, typename Option>
indexlib::file_system::ReadOption
KKVBuiltSegmentIterator<SKeyType, Option>::CreateReadOption(KVMetricsCollector* metricsCollector)
{
    auto readOption = indexlib::file_system::ReadOption::LowLatency();
    if (metricsCollector) {
        readOption.blockCounter = metricsCollector->GetBlockCounter();
    }
    return readOption;
}

} // namespace indexlibv2::index
