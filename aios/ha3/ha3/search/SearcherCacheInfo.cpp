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
#include "ha3/search/SearcherCacheInfo.h"

#include <assert.h>
#include <algorithm>
#include <memory>

#include "alog/Logger.h"
#include "autil/RangeUtil.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/CacheMinScoreFilter.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/DefaultSearcherCacheStrategy.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/HitCollectorManager.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/SearcherCache.h"
#include "ha3/search/SearcherCacheManager.h"
#include "ha3/search/SortExpression.h"

namespace isearch {
namespace rank {
class HitCollectorBase;
}  // namespace rank
}  // namespace isearch

using namespace isearch::monitor;
using namespace isearch::common;
using namespace isearch::rank;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SearcherCacheInfo);

void SearcherCacheInfo::fillAfterRerank(uint32_t actualMatchDocs) {
    if (!isHit) {
        isTruncated = extraRankCount < actualMatchDocs;
    }
}

void SearcherCacheInfo::fillAfterSeek(HitCollectorManager *hitCollectorManager) {
    if (!isHit) {
        CacheMinScoreFilter scoresFilter;
        scoresFilter.storeMinScore(hitCollectorManager->getRankHitCollector(),
                hitCollectorManager->getFirstExpressions());
        scores = scoresFilter.getMinScoreVec();
    }
}

void SearcherCacheInfo::fillAfterStealIfHit(const common::Request *request,
        InnerSearchResult& innerResult,
        HitCollectorManager *hitCollectorManager)
{
    auto *collector = cacheManager->getMetricsCollector();
    collector->increaseCacheHitNum();
    auto searcherCache = cacheManager->getSearcherCache();
    CacheResult *cacheResult = cacheManager->getCacheResult();
    AUTIL_LOG(DEBUG, "seek from inc, totalMatchDocs:[%d], matchDocCount:[%zd]",
            innerResult.totalMatchDocs, innerResult.matchDocVec.size());
    if (searcherCache->exceedIncDocLimit(innerResult.totalMatchDocs)) {
        common::SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
        assert(cacheClause);
        uint64_t key = cacheClause->getKey();
        autil::PartitionRange partRange = cacheClause->getPartRange();
        searcherCache->deleteCacheItem(key, partRange);
    }

    if (innerResult.matchDocVec.size() > 0) {
        size_t expectCount = getExpectCount(docCountLimits.runtimeTopK,
                cacheResult);
        SortExpressionVector allFirstExpressions =
            hitCollectorManager->getFirstExpressions();
        HitCollectorBase *rankCollector =
            hitCollectorManager->getRankHitCollector();
        cacheResult->getHeader()->minScoreFilter.filterByMinScore(
                rankCollector, allFirstExpressions, innerResult.matchDocVec,
                expectCount);
    }

}
void SearcherCacheInfo::fillAfterStealIfMiss(const common::Request *request,
        InnerSearchResult& innerResult)
{
    const DefaultSearcherCacheStrategyPtr& cacheStrategy = cacheManager->getCacheStrategy();
    extraRankCount = std::max(docCountLimits.requiredTopK, cacheStrategy->getCacheDocNum(request,
                    docCountLimits.requiredTopK));
    extraRankCount = std::min(extraRankCount, (uint32_t)innerResult.matchDocVec.size());
}

uint64_t SearcherCacheInfo::getKey(const common::Request *request) {
    common::SearcherCacheClause *cacheClause =
        request->getSearcherCacheClause();
    assert(cacheClause);
    return cacheClause->getKey();
}

size_t SearcherCacheInfo::getExpectCount(uint32_t topK, CacheResult *cacheResult) {
    assert(cacheResult);
    common::Result *result = cacheResult->getResult();
    if (result == NULL) {
        return 0;
    }
    MatchDocs *matchDocs = result->getMatchDocs();
    if (matchDocs == NULL) {
        return topK;
    }
    if (matchDocs->size() < topK) {
        return topK - matchDocs->size();
    }
    return 0;
}

} // namespace search
} // namespace isearch
