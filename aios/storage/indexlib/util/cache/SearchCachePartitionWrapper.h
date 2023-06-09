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

#include "autil/Log.h"
#include "indexlib/util/cache/SearchCache.h"

namespace indexlib { namespace util {
class SearchCachePartitionWrapper
{
private:
    struct CacheKey {
        uint64_t key;
        uint64_t partitionId;
    };

public:
    SearchCachePartitionWrapper(const SearchCachePtr& searchCache, const std::string& partitionName);
    ~SearchCachePartitionWrapper();

public:
    typedef int32_t regionid_t;

    template <typename CacheItem>
    bool Put(uint64_t key, regionid_t regionId, CacheItem* cacheItem, autil::CacheBase::Priority priority)
    {
        assert(_searchCache);
        CacheKey cacheKey = MakeKey(key, regionId);
        autil::StringView constStringKey((const char*)&cacheKey, sizeof(CacheKey));
        return _searchCache->Put(constStringKey, cacheItem, priority);
    }

    std::unique_ptr<CacheItemGuard> Get(uint64_t key, regionid_t regionId,
                                        SearchCacheCounter* searchCacheCounter = nullptr)
    {
        assert(_searchCache);
        CacheKey cacheKey = MakeKey(key, regionId);
        autil::StringView constStringKey((const char*)&cacheKey, sizeof(CacheKey));
        return _searchCache->Get(constStringKey, searchCacheCounter);
    }

    size_t GetUsage() const
    {
        assert(_searchCache);
        return _searchCache->GetUsage();
    }

    SearchCachePtr GetSearchCache() const { return _searchCache; }

    SearchCachePartitionWrapper* CreateExclusiveCacheWrapper(const std::string& id);

private:
    void InitPartitionId();
    CacheKey MakeKey(uint64_t key, regionid_t regionId)
    {
        CacheKey cacheKey;
        cacheKey.key = key;
        cacheKey.partitionId = _partitionId ^ regionId;
        return cacheKey;
    }

private:
    SearchCachePtr _searchCache;
    uint64_t _partitionId;

    std::string _partitionName;
    std::string _exclusiveId;
    uint64_t _timestamp;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearchCachePartitionWrapper> SearchCachePartitionWrapperPtr;
}} // namespace indexlib::util
