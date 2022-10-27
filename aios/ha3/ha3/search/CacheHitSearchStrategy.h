#ifndef ISEARCH_CACHEHITSEARCHSTRATEGY_H
#define ISEARCH_CACHEHITSEARCHSTRATEGY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocSearchStrategy.h>
#include <suez/turing/expression/plugin/SorterManager.h>

namespace autil {
namespace mem_pool {
class Pool;
};
};

BEGIN_HA3_NAMESPACE(search);

class SearcherCacheManager;
class HitCollectorManager;
class CacheResult;

class CacheHitSearchStrategy : public MatchDocSearchStrategy
{
public:
    CacheHitSearchStrategy(
            const DocCountLimits& docCountLimits,
            const common::Request *request,
            const SearcherCacheManager *cacheManager,
            const suez::turing::SorterManager *sorterManager,
            autil::mem_pool::Pool *pool,
            std::string mainTable,
            IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot);

    ~CacheHitSearchStrategy() override;
private:
    CacheHitSearchStrategy(const CacheHitSearchStrategy &);
    CacheHitSearchStrategy& operator=(const CacheHitSearchStrategy &);
public:
    void afterStealFromCollector(InnerSearchResult& innerResult,
                                 HitCollectorManager *hitCollectorManager) override;
private:
    common::Result *reconstructResult(common::Result *result) override;

    static uint64_t getKey(const common::Request *request) {
        common::SearcherCacheClause *cacheClause =
            request->getSearcherCacheClause();
        assert(cacheClause);
        return cacheClause->getKey();
    }
    static size_t getExpectCount(uint32_t topK, CacheResult *cacheResult);
    common::Result* mergeResult(common::Result *fullResult,
                                common::Result *incResult,
                                const common::Request *request,
                                uint32_t requireTopK);
    void refreshAttributes(common::Result *result);
    void initRefreshAttrs(const common::Ha3MatchDocAllocatorPtr& vsa,
                          suez::turing::JoinDocIdConverterCreator *docIdConverterFactory,
                          suez::turing::AttributeExpressionFactory *attrExprFactory,
                          common::SearcherCacheClause *searcherCacheClause,
                          suez::turing::ExpressionVector &needRefreshAttrs);
    void createNeedRefreshAttrExprs(
            const common::Ha3MatchDocAllocatorPtr &allocator,
            suez::turing::JoinDocIdConverterCreator *docIdConverterFactory,
            suez::turing::AttributeExpressionFactory *attrExprFactory,
            const std::vector<std::string> &needRefreshAttrNames,
            suez::turing::ExpressionVector &needRefreshAttrs);
private:
    const common::Request *_request = nullptr;
    const SearcherCacheManager *_cacheManager = nullptr;
    const suez::turing::SorterManager *_sorterManager = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
    std::string _mainTable;
    IE_NAMESPACE(partition)::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
private:
    friend class CacheHitSearchStrategyTest;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_CACHEHITSEARCHSTRATEGY_H
