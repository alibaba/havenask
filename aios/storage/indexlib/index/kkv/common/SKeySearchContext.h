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

#include <cassert>
#include <unordered_set>
#include <vector>

#include "autil/mem_pool/pool_allocator.h"

namespace indexlibv2::index {

template <typename SKeyType>
class SKeySearchContext
{
public:
    using SKeyVector = std::vector<SKeyType>;
    using PooledSKeyVector = std::vector<SKeyType, autil::mem_pool::pool_allocator<SKeyType>>;
    using SKeySet = std::unordered_set<SKeyType>;
    using PooledSKeySet = std::unordered_set<SKeyType, std::hash<SKeyType>, std::equal_to<SKeyType>,
                                             autil::mem_pool::pool_allocator<SKeyType>>;

public:
    SKeySearchContext(autil::mem_pool::Pool* pool)
        : _requiredSkeys(autil::mem_pool::pool_allocator<SKeyType>(pool))
        , _sortedRequiredSkeys(autil::mem_pool::pool_allocator<SKeyType>(pool))
        , _maxRequiredSkey(std::numeric_limits<SKeyType>::min())
    {
    }

    ~SKeySearchContext() {}

public:
    template <typename Alloc>
    void Init(const std::vector<uint64_t, Alloc>& suffixKeyHashVec);

    const PooledSKeySet& GetRequiredSKeys() const { return _requiredSkeys; }
    SKeyType GetMaxRequiredSKey() const { return _maxRequiredSkey; }
    bool MatchRequiredSKey(SKeyType key) const { return _requiredSkeys.find(key) != _requiredSkeys.end(); }
    size_t GetRequiredSKeyCount() const { return _requiredSkeys.size(); }
    bool MatchAllRequiredSKeys(size_t foundSkeyCount) const { return _requiredSkeys.size() == foundSkeyCount; }
    const PooledSKeyVector& GetSortedRequiredSKeys() const { return _sortedRequiredSkeys; }

private:
    PooledSKeySet _requiredSkeys;
    PooledSKeyVector _sortedRequiredSkeys;
    SKeyType _maxRequiredSkey;
};

/////////////////////////////////////////////////////////////////////////////
template <typename SKeyType>
template <typename Alloc>
void SKeySearchContext<SKeyType>::Init(const std::vector<uint64_t, Alloc>& suffixKeyHashVec)
{
    assert(!suffixKeyHashVec.empty());
    _requiredSkeys.reserve(suffixKeyHashVec.size());
    _sortedRequiredSkeys.reserve(suffixKeyHashVec.size());
    for (size_t i = 0; i < suffixKeyHashVec.size(); ++i) {
        SKeyType skey = (SKeyType)suffixKeyHashVec[i];
        if (skey > _maxRequiredSkey) {
            _maxRequiredSkey = skey;
        }
        if (_requiredSkeys.insert(skey).second) {
            _sortedRequiredSkeys.push_back(skey);
        }
    }
    if (_sortedRequiredSkeys.size() > 1) {
        std::sort(_sortedRequiredSkeys.begin(), _sortedRequiredSkeys.end());
    }
}

} // namespace indexlibv2::index
