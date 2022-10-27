#include <ha3/search/SearcherCacheInfo.h>
#include <ha3/search/HitCollectorManager.h>
#include <ha3/search/CacheMinScoreFilter.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/search/SearcherCacheManager.h>
#include <ha3/common.h>

USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SearcherCacheInfo);

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
    HA3_LOG(DEBUG, "seek from inc, totalMatchDocs:[%d], "
            "matchDocCount:[%zd]",
            innerResult.totalMatchDocs, innerResult.matchDocVec.size());
    if (searcherCache->exceedIncDocLimit(innerResult.totalMatchDocs)) {
        searcherCache->deleteCacheItem(getKey(request));
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
    Result* result = cacheResult->getResult();
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

END_HA3_NAMESPACE(search);
