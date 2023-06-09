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
#include "ha3/search/CacheMissSearchStrategy.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/CacheMinScoreFilter.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/DefaultSearcherCacheStrategy.h"
#include "ha3/search/HitCollectorManager.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearchStrategy.h"
#include "ha3/search/SearcherCache.h"
#include "ha3/search/SearcherCacheManager.h"
//#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "matchdoc/SubDocAccessor.h"

using namespace std;

using namespace isearch::common;
using namespace isearch::monitor;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, CacheMissSearchStrategy);

CacheMissSearchStrategy::CacheMissSearchStrategy(
        const DocCountLimits &docCountLimits,
        const Request *request,
        const SearcherCacheManager *cacheManager)
    : MatchDocSearchStrategy(docCountLimits)
    , _request(request)
    , _cacheManager(cacheManager)
    , _latency(autil::TimeUtility::currentTime())
    , _extraRankCount(0)
    , _isTruncated(false)
{ }

CacheMissSearchStrategy::~CacheMissSearchStrategy() {
}

void CacheMissSearchStrategy::afterSeek(HitCollectorManager *hitCollectorManager) {
    // get min first scorer's score, it must be before rerank
    _minScoreFilter.storeMinScore(hitCollectorManager->getRankHitCollector(),
                                  hitCollectorManager->getFirstExpressions());
}

void CacheMissSearchStrategy::afterStealFromCollector(InnerSearchResult& innerResult, HitCollectorManager *hitCollectorManager) {
    const DefaultSearcherCacheStrategyPtr& cacheStrategy =
        _cacheManager->getCacheStrategy();
    _extraRankCount = max(_docCountLimits.requiredTopK,
                          cacheStrategy->getCacheDocNum(_request,
                                  _docCountLimits.requiredTopK));
    _extraRankCount = min(_extraRankCount,
                          (uint32_t)innerResult.matchDocVec.size());
}

void CacheMissSearchStrategy::afterRerank(InnerSearchResult& innerResult) {
    _isTruncated = _extraRankCount < innerResult.actualMatchDocs;
}

void CacheMissSearchStrategy::afterExtraRank(InnerSearchResult& innerResult) {
    auto searcherCache = _cacheManager->getSearcherCache();
    uint32_t latencyLimit =
        searcherCache->getCacheLatencyLimit();
    _latency = autil::TimeUtility::currentTime() - _latency;
    _needPut = _latency > (uint64_t)latencyLimit;
    AUTIL_LOG(DEBUG, "search latency: %ld, latency limt:%d",
            _latency, latencyLimit);
}

common::Result *CacheMissSearchStrategy::reconstructResult(common::Result *result) {
    assert(result);
    assert(result->getMatchDocs());
    if (!_needPut) {
        return result;
    }
    SessionMetricsCollector *collector =
        _cacheManager->getMetricsCollector();
    result->setUseTruncateOptimizer(collector->useTruncateOptimizer());
    CacheResult *cacheResult =
        constructCacheResult(result, _minScoreFilter,
                             _cacheManager->getIndexPartReaderWrapper());
    assert(cacheResult);
    cacheResult->setTruncated(_isTruncated);
    cacheResult->setUseTruncateOptimizer(
            collector->useTruncateOptimizer());
    SearcherCacheClause *cacheClause =
        _request->getSearcherCacheClause();
    uint64_t key = cacheClause->getKey();
    autil::PartitionRange partRange = cacheClause->getPartRange();
    uint32_t curTime = cacheClause->getCurrentTime();
    auto searcherCache = _cacheManager->getSearcherCache();
    const DefaultSearcherCacheStrategyPtr& cacheStrategy =
        _cacheManager->getCacheStrategy();
    bool ret = searcherCache->put(key, partRange, curTime, cacheStrategy,
                                  cacheResult, _request->getPool());
    if (ret) {
        collector->increaseCachePutNum();
        uint64_t memUse;
        uint32_t itemNum;
        searcherCache->getCacheStat(memUse, itemNum);
        collector->setCacheMemUse(memUse);
        collector->setCacheItemNum(itemNum);
    }

    result = cacheResult->stealResult();
    truncateResult(_docCountLimits.requiredTopK, result);
    delete cacheResult;
    return result;
}

CacheResult *CacheMissSearchStrategy::constructCacheResult(
    common::Result *result,
    const CacheMinScoreFilter &minScoreFilter,
    const IndexPartitionReaderWrapperPtr &idxPartReaderWrapper) {
    CacheResult* cacheResult = new CacheResult;
    CacheHeader* header = cacheResult->getHeader();
    header->minScoreFilter = minScoreFilter;

    cacheResult->setResult(result);

    const shared_ptr<PartitionInfoWrapper> &partInfoPtr =
        idxPartReaderWrapper->getPartitionInfo();
    const indexlib::index::PartitionInfoHint &partInfoHint = partInfoPtr->GetPartitionInfoHint();
    cacheResult->setPartInfoHint(partInfoHint);

    vector<globalid_t> &gids = cacheResult->getGids();
    fillGids(result, partInfoPtr, gids);

    MatchDocs *matchDocs = result->getMatchDocs();
    if (matchDocs) {
        const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
        if (matchDocAllocator && matchDocAllocator->hasSubDocAllocator()) {
            const indexlib::index::PartitionInfoPtr &subPartitionInfoPtr =
                idxPartReaderWrapper->getSubPartitionInfo();
            vector<globalid_t> &subDocGids = cacheResult->getSubDocGids();
            fillSubDocGids(result, subPartitionInfoPtr, subDocGids);
        }
    }
    return cacheResult;
}

void CacheMissSearchStrategy::fillGids(const common::Result *result,
                                       const shared_ptr<PartitionInfoWrapper> &partInfoPtr,
                                       vector<globalid_t> &gids) {
    gids.clear();

    MatchDocs *matchDocs = result->getMatchDocs();
    if (!matchDocs || !partInfoPtr.get()) {
        return;
    }

    uint32_t size = matchDocs->size();
    gids.reserve(size);
    for (uint32_t i = 0; i < size; i++) {
        docid_t docId = matchDocs->getDocId(i);
        globalid_t gid = partInfoPtr->GetGlobalId(docId);
        gids.push_back(gid);
    }
}

void CacheMissSearchStrategy::fillSubDocGids(const common::Result *result,
                                             const indexlib::index::PartitionInfoPtr &partInfoPtr,
                                             vector<globalid_t> &subDocGids) {
    subDocGids.clear();
    if (!partInfoPtr.get()) {
        return;
    }
    MatchDocs *matchDocs = result->getMatchDocs();
    assert(matchDocs);
    const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
    assert(matchDocAllocator);
    auto accessor = matchDocAllocator->getSubDocAccessor();
    assert(accessor != NULL);
    uint32_t size = matchDocs->size();
    for (uint32_t i = 0; i < size; i++) {
        auto matchDoc = matchDocs->getMatchDoc(i);
        vector<int32_t> subDocids;
        accessor->getSubDocIds(matchDoc, subDocids);
        for (auto id : subDocids) {
            subDocGids.push_back(partInfoPtr->GetGlobalId(id));
        }
    }
}

} // namespace search
} // namespace isearch
