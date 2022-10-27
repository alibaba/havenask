#ifndef ISEARCH_CACHEMISSSEARCHSTRATEGY_H
#define ISEARCH_CACHEMISSSEARCHSTRATEGY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocSearchStrategy.h>
#include <ha3/search/CacheMinScoreFilter.h>

BEGIN_HA3_NAMESPACE(search);

class SearcherCacheManager;
class HitCollectorManager;
class CacheResult;

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
                  const index::PartitionInfoPtr &partInfoPtr,
                  std::vector<globalid_t> &gids);
    static void fillSubDocGids(const common::Result *result,
                        const index::PartitionInfoPtr &partInfoPtr,
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
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CacheMissSearchStrategy);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_CACHEMISSSEARCHSTRATEGY_H
