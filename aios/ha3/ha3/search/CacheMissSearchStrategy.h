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

#include <stdint.h>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Result.h"
#include "ha3/search/CacheMinScoreFilter.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/MatchDocSearchStrategy.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

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

class CacheResult;
class HitCollectorManager;
class SearcherCacheManager;

class CacheMissSearchStrategy : public MatchDocSearchStrategy
{
public:
    CacheMissSearchStrategy(
            const DocCountLimits &docCountLimits,
            const common::Request *request,
            const SearcherCacheManager *cacheManager);

    ~CacheMissSearchStrategy() override;
private:
    CacheMissSearchStrategy(const CacheMissSearchStrategy &);
    CacheMissSearchStrategy& operator=(const CacheMissSearchStrategy &);
public:
    void afterSeek(HitCollectorManager *hitCollectorManager) override;
    void afterStealFromCollector(InnerSearchResult& innerResult, HitCollectorManager *hitCollectorManager) override;
    void afterRerank(InnerSearchResult& innerResult) override;
    void afterExtraRank(InnerSearchResult& innerResult) override;
    common::Result* reconstructResult(common::Result* result) override;

    uint32_t getExtraRankCount() override {
        return _extraRankCount;
    }
    uint32_t getResultCount() override {
        if (_needPut) {
            return _extraRankCount;
        }
        return _docCountLimits.requiredTopK;
    }
private:
    static CacheResult* constructCacheResult(common::Result *result,
            const CacheMinScoreFilter &minScoreFilter,
            const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper);
    static void fillGids(const common::Result *result,
                         const std::shared_ptr<PartitionInfoWrapper> &partInfoPtr,
                         std::vector<globalid_t> &gids);
    static void fillSubDocGids(const common::Result *result,
                               const indexlib::index::PartitionInfoPtr &partInfoPtr,
                               std::vector<globalid_t> &subDocGids);
private:
    const common::Request *_request;
    const SearcherCacheManager *_cacheManager;
    CacheMinScoreFilter _minScoreFilter;
    uint64_t _latency;
    uint32_t _extraRankCount;
    bool _needPut;
    bool _isTruncated;
private:
    friend class CacheMissSearchStrategyTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CacheMissSearchStrategy> CacheMissSearchStrategyPtr;

} // namespace search
} // namespace isearch
