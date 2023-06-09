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
#ifndef __INDEXLIB_KKV_SEGMENT_ITERATOR_H
#define __INDEXLIB_KKV_SEGMENT_ITERATOR_H

#include <memory>
#include <unordered_set>

#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/skey_search_context.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KKVSegmentIterator
{
public:
    inline KKVSegmentIterator(autil::mem_pool::Pool* sessionPool)
        : mPool(sessionPool)
        , mIsLastSeg(false)
        , mKeepSortSequence(false)
        , mKKVIndexConfig(nullptr)
    {
    }
    virtual ~KKVSegmentIterator() {}

public:
    virtual bool IsValid() const = 0;
    virtual bool HasPKeyDeleted() const = 0;
    virtual uint32_t GetPKeyDeletedTs() const = 0;
    virtual bool MoveToNext() = 0;
    virtual void GetCurrentSkey(uint64_t& skey, bool& isDeleted) const = 0;
    virtual void GetCurrentValue(autil::StringView& value) = 0;
    virtual uint32_t GetCurrentTs() = 0;
    virtual uint32_t GetCurrentExpireTime() = 0;

    // compare current skey value
    // return 0 : self.skey == other.skey
    // return -1 : self.skey  < other.skey
    // return 1 : self.skey > other.skey
    virtual int Compare(const KKVSegmentIterator* other) = 0;

    void SetIsLastSegment(bool isLastSeg) { mIsLastSeg = isLastSeg; }

    virtual bool CurrentSKeyExpired(uint64_t currentTsInSecond)
    {
        return SKeyExpired(*mKKVIndexConfig, currentTsInSecond, GetCurrentTs(), GetCurrentExpireTime());
    }

protected:
    enum SKeyStatus {
        VALID = 0,
        FILTERED,
        INVALID,
    };

protected:
    autil::mem_pool::Pool* mPool;
    bool mIsLastSeg;
    bool mKeepSortSequence;
    const config::KKVIndexConfig* mKKVIndexConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVSegmentIterator);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_SEGMENT_ITERATOR_H
