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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class SKeySearchContext
{
private:
    using PooledSKeySet = typename SKeyHashContainerTraits<SKeyType>::PooledSKeySet;
    using PooledSKeyVector = typename SKeyHashContainerTraits<SKeyType>::PooledSKeyVector;

public:
    inline SKeySearchContext(autil::mem_pool::Pool* pool)
        : mRequiredSkeys(autil::mem_pool::pool_allocator<SKeyType>(pool))
        , mSortedRequiredSkeys(autil::mem_pool::pool_allocator<SKeyType>(pool))
        , mMaxRequiredSkey(std::numeric_limits<SKeyType>::min())
        , mNeedSeekBySKey(false)
        , mCurrentSeekSKeyPos(0)
    {
    }

    ~SKeySearchContext() {}

public:
    template <typename Alloc>
    inline void Init(const std::vector<uint64_t, Alloc>& suffixKeyHashVec);

    inline const PooledSKeySet& GetRequiredSKeys() const { return mRequiredSkeys; }
    inline const PooledSKeyVector& GetSortedRequiredSKeys() const { return mSortedRequiredSkeys; }
    inline SKeyType GetMaxRequiredSKey() const { return mMaxRequiredSkey; }

    inline bool MatchRequiredSKey(SKeyType key) const { return mRequiredSkeys.find(key) != mRequiredSkeys.end(); }

    inline bool NeedSeekBySKey() const { return mNeedSeekBySKey; }
    inline void UpdateNeedSeekBySKey(bool flag) { mNeedSeekBySKey = flag; }

    inline size_t GetCurrentSeekSKeyPos() const { return mCurrentSeekSKeyPos; }
    inline void IncCurrentSeekSKeyPos() { ++mCurrentSeekSKeyPos; }

    inline void ResetSKeySKipInfo()
    {
        mNeedSeekBySKey = false;
        mCurrentSeekSKeyPos = 0;
    }

    inline size_t GetRequiredSKeyCount() const { return mRequiredSkeys.size(); }

    inline bool MatchAllRequiredSKeys(size_t foundSkeyCount) const { return mRequiredSkeys.size() == foundSkeyCount; }

private:
    PooledSKeySet mRequiredSkeys;
    PooledSKeyVector mSortedRequiredSkeys;
    SKeyType mMaxRequiredSkey;
    bool mNeedSeekBySKey;
    size_t mCurrentSeekSKeyPos;

private:
    IE_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////////////
template <typename SKeyType>
template <typename Alloc>
inline void SKeySearchContext<SKeyType>::Init(const std::vector<uint64_t, Alloc>& suffixKeyHashVec)
{
    assert(!suffixKeyHashVec.empty());
    mRequiredSkeys.reserve(suffixKeyHashVec.size());
    mSortedRequiredSkeys.reserve(suffixKeyHashVec.size());
    for (size_t i = 0; i < suffixKeyHashVec.size(); ++i) {
        SKeyType skey = (SKeyType)suffixKeyHashVec[i];
        if (skey > mMaxRequiredSkey) {
            mMaxRequiredSkey = skey;
        }
        if (mRequiredSkeys.insert(skey).second) {
            mSortedRequiredSkeys.push_back(skey);
        }
    }
    if (mSortedRequiredSkeys.size() > 1) {
        std::sort(mSortedRequiredSkeys.begin(), mSortedRequiredSkeys.end());
    }
}
}} // namespace indexlib::index
