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

#include "indexlib/index/kkv/common/KKVTTLHelper.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVSegmentIteratorBase
{
public:
    inline KKVSegmentIteratorBase(autil::mem_pool::Pool* pool, const config::KKVIndexConfig* indexConfig,
                                  bool keepSortSequence = false)
        : _pool(pool)
        , _indexConfig(indexConfig)
        , _ttl(_indexConfig->GetTTL())
        , _keepSortSeq(keepSortSequence)
    {
    }
    virtual ~KKVSegmentIteratorBase() {}

public:
    bool IsValid() const { return _isValid; }
    bool HasPKeyDeleted() const { return _hasPKeyDeleted; }
    uint32_t GetPKeyDeletedTs() const { return _pkeyDeletedTs; }
    SKeyType GetCurrentSkey() const { return _skey; }
    bool CurrentSkeyDeleted() const { return _skeyDeleted; }
    uint32_t GetCurrentTs() const { return _timestamp; }
    uint32_t GetCurrentExpireTime() const { return _expireTime; }

public:
    virtual void MoveToNext() = 0;
    virtual void BatchGet(SKeySearchContext<SKeyType>* skeyContext, uint64_t minTsInSecond, uint64_t curTsInSecond,
                          typename SKeySearchContext<SKeyType>::PooledSKeySet& foundSKeys,
                          KKVResultBuffer& resultBuffer) = 0;
    virtual void GetCurrentValue(autil::StringView& value) = 0;

public:
    void SetIsLastSegment(bool isLastSeg) { _isLastSeg = isLastSeg; }
    bool IsLastSegment() const { return _isLastSeg; }
    void SetIsOnlySegment(bool isOnlySeg) { _isOnlySeg = isOnlySeg; }
    bool IsOnlySegment() const { return _isOnlySeg; }

protected:
    inline bool SKeyExpired(uint64_t curTsInSecond, uint64_t docTsInSecond, uint64_t docExpireTime) const
    {
        return KKVTTLHelper::SKeyExpired(curTsInSecond, docTsInSecond, docExpireTime, _ttl);
    }

protected:
    autil::mem_pool::Pool* _pool = nullptr;
    const config::KKVIndexConfig* _indexConfig = nullptr;
    int64_t _ttl = 0;
    bool _isLastSeg = false;
    bool _isOnlySeg = false;
    bool _keepSortSeq = false;

protected:
    bool _isValid = false;
    bool _hasPKeyDeleted = false;
    uint32_t _pkeyDeletedTs = 0;
    SKeyType _skey = SKeyType();
    bool _skeyDeleted = false;
    uint32_t _timestamp = 0;
    uint32_t _expireTime = indexlib::UNINITIALIZED_EXPIRE_TIME;
};

} // namespace indexlibv2::index
