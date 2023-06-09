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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/isearch.h"
#include "ha3/search/DocCountLimits.h"

namespace isearch {
namespace common {
class Request;
}  // namespace common
namespace search {
struct InnerSearchResult;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace search {

class HitCollectorManager;
class SearcherCacheManager;

typedef std::shared_ptr<SearcherCacheManager> SearcherCacheManagerPtr;

class CacheResult;

class SearcherCacheInfo {
public:
    SearcherCacheInfo()
        : isHit(false)
        , isTruncated(false)
        , extraRankCount(0)
        , beginTime(autil::TimeUtility::currentTime())
    {}
    search::SearcherCacheManagerPtr cacheManager;
    bool isHit;
    DocCountLimits docCountLimits;
    // cache miss
    bool isTruncated;
    uint32_t extraRankCount;
    uint64_t beginTime;
    std::vector<score_t> scores;
public:
    void fillAfterRerank(uint32_t actualMatchDocs);
    void fillAfterSeek(HitCollectorManager *hitCollectorManager);
    void fillAfterStealIfHit(const common::Request *request,
                             InnerSearchResult& innerResult,
                             HitCollectorManager *hitCollectorManager);
    void fillAfterStealIfMiss(const common::Request *request, InnerSearchResult& innerResult);
private:
    static uint64_t getKey(const common::Request *request);
    size_t getExpectCount(uint32_t topK, CacheResult *cacheResult);
private:
    AUTIL_LOG_DECLARE();    
};

typedef std::shared_ptr<SearcherCacheInfo> SearcherCacheInfoPtr;

} // namespace search
} // namespace isearch


