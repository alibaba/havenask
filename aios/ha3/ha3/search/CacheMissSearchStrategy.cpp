#include <ha3/search/CacheMissSearchStrategy.h>
#include <ha3/search/SearcherCache.h>
#include <ha3/search/SearcherCacheManager.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <autil/TimeUtility.h>

using namespace std;

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, CacheMissSearchStrategy);

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
    HA3_LOG(DEBUG, "search latency: %ld, latency limt:%d",
            _latency, latencyLimit);
}

Result* CacheMissSearchStrategy::reconstructResult(Result* result) {
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
    uint32_t curTime = cacheClause->getCurrentTime();
    auto searcherCache = _cacheManager->getSearcherCache();
    const DefaultSearcherCacheStrategyPtr& cacheStrategy =
        _cacheManager->getCacheStrategy();
    bool ret = searcherCache->put(key, curTime, cacheStrategy,
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

CacheResult* CacheMissSearchStrategy::constructCacheResult(
        Result *result,
        const CacheMinScoreFilter &minScoreFilter,
        const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper)
{
    CacheResult* cacheResult = new CacheResult;
    CacheHeader* header = cacheResult->getHeader();
    header->minScoreFilter = minScoreFilter;

    cacheResult->setResult(result);

    const PartitionInfoPtr &partInfoPtr =
        idxPartReaderWrapper->getPartitionInfo();
    const PartitionInfoHint &partInfoHint = partInfoPtr->GetPartitionInfoHint();
    cacheResult->setPartInfoHint(partInfoHint);

    vector<globalid_t> &gids = cacheResult->getGids();
    fillGids(result, partInfoPtr, gids);

    MatchDocs *matchDocs = result->getMatchDocs();
    if (matchDocs) {
        const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
        if (matchDocAllocator && matchDocAllocator->hasSubDocAllocator()) {
            const PartitionInfoPtr &subPartitionInfoPtr =
                idxPartReaderWrapper->getSubPartitionInfo();
            vector<globalid_t> &subDocGids = cacheResult->getSubDocGids();
            fillSubDocGids(result, subPartitionInfoPtr, subDocGids);
        }
    }
    return cacheResult;
}

void CacheMissSearchStrategy::fillGids(const Result *result,
                                        const PartitionInfoPtr &partInfoPtr,
                                        vector<globalid_t> &gids)
{
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

void CacheMissSearchStrategy::fillSubDocGids(const Result *result,
        const PartitionInfoPtr &partInfoPtr,
        vector<globalid_t> &subDocGids)
{
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

END_HA3_NAMESPACE(search);
