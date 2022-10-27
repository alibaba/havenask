#ifndef ISEARCH_SEARCHERCACHEINFO_H
#define ISEARCH_SEARCHERCACHEINFO_H

#include <autil/TimeUtility.h>
#include <ha3/common.h>
#include <ha3/common/Request.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/search/DocCountLimits.h>
BEGIN_HA3_NAMESPACE(search);

class HitCollectorManager;
class SearcherCacheManager;
HA3_TYPEDEF_PTR(SearcherCacheManager);

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
    HA3_LOG_DECLARE();    
};

HA3_TYPEDEF_PTR(SearcherCacheInfo);

END_HA3_NAMESPACE(search);


#endif
