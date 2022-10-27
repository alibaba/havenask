#ifndef ISEARCH_SEARCHERCACHEMANAGER_H
#define ISEARCH_SEARCHERCACHEMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearcherCache.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/DefaultSearcherCacheStrategy.h>
#include <ha3/search/DocCountLimits.h>
#include <ha3/common/Request.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <suez/turing/expression/function/FunctionManager.h>

namespace autil {
namespace mem_pool {
class Pool;
};
};

BEGIN_HA3_NAMESPACE(monitor);
class SessionMetricsCollector;
END_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(search);

class HitCollectorManager;
class MatchDocSearchStrategy;

class SearcherCacheManager
{
public:
    SearcherCacheManager(SearcherCache *searcherCache,
                         const IndexPartitionReaderWrapperPtr &indexPartReaderWrapper,
                         autil::mem_pool::Pool *pool,
                         bool hasSubSchema,
                         SearchCommonResource &resource,
                         SearchPartitionResource &searchPartitionResource);
    ~SearcherCacheManager();
private:
    SearcherCacheManager(const SearcherCacheManager &);
    SearcherCacheManager& operator=(const SearcherCacheManager &);
public:
    bool initCacheBeforeOptimize(const common::Request *request,
                                 monitor::SessionMetricsCollector *sessionMetricsCollector,
                                 common::MultiErrorResult *errorResult,
                                 uint32_t requiredTopK);
    inline bool useCache(const common::Request *request) const {
	auto *auxQueryClause = request->getAuxQueryClause();
	auto *auxFilterClause = request->getAuxFilterClause();
	if (auxQueryClause || auxFilterClause) {
	    return false;
	}

        common::SearcherCacheClause *cacheClause =
            request->getSearcherCacheClause();
        if (!cacheClause) {
            return false;
        }
        return cacheClause->getUse() && _searcherCache;
    }
    inline bool cacheHit() const {
        return _cacheResult != NULL;
    }
    SearcherCache *getSearcherCache() const {
        return _searcherCache;
    }
    CacheResult* getCacheResult() const {
        return _cacheResult;
    }
    const DefaultSearcherCacheStrategyPtr& getCacheStrategy() const {
        return _cacheStrategy;
    }
    monitor::SessionMetricsCollector* getMetricsCollector() const {
        return _sessionMetricsCollector;
    }
    const IndexPartitionReaderWrapperPtr& getIndexPartReaderWrapper() const {
        return _indexPartReaderWrapper;
    }
    MatchDocSearchStrategy *getSearchStrategy(
            const suez::turing::SorterManager *sorterManager,
            const common::Request *request,
            const DocCountLimits& docCountLimits) const;
private:
    static common::QueryLayerClause* createLayerClause(
            const DocIdRangeVector& docIdRanges);
    static bool modifyRequest(const common::Request *request,
                              CacheResult *cacheResult,
                              const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper);
    static void recoverDocIds(CacheResult *cacheResult,
                              const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper);
    static void recoverSubDocIds(common::MatchDocs *matchDocs,
                                 const std::vector<globalid_t> &subDocGids,
                                 const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper);
    void reportCacheMissMetrics(CacheMissType missType);
private:
    SearcherCache *_searcherCache = nullptr;
    DefaultSearcherCacheStrategyPtr _cacheStrategy;
    CacheResult* _cacheResult = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
    IndexPartitionReaderWrapperPtr _indexPartReaderWrapper;
    suez::turing::FunctionManager *_funcManager = nullptr;
    monitor::SessionMetricsCollector *_sessionMetricsCollector = nullptr;
    std::string _mainTable;
    IE_NAMESPACE(partition)::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
    common::Tracer *_tracer = nullptr;
private:
    friend class SearcherCacheManagerTest;
    friend class CachedMatchDocSearcherTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherCacheManager);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEARCHERCACHEMANAGER_H
