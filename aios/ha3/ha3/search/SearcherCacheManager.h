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
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Request.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/Tracer.h"
#include "ha3/search/DefaultSearcherCacheStrategy.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearcherCache.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
}  // namespace partition
}  // namespace indexlib
namespace isearch {
namespace common {
class MatchDocs;
class MultiErrorResult;
class QueryLayerClause;
}  // namespace common
namespace search {
class CacheResult;
class SearchCommonResource;
class SearchPartitionResource;
struct DocCountLimits;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class FunctionManager;
class SorterManager;
}  // namespace turing
}  // namespace suez

namespace autil {
namespace mem_pool {
class Pool;
};
};

namespace isearch {
namespace monitor {
class SessionMetricsCollector;

} // namespace monitor
} // namespace isearch

namespace isearch {
namespace search {

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
            const indexlib::DocIdRangeVector& docIdRanges);
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
    indexlib::partition::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
    common::Tracer *_tracer = nullptr;
private:
    friend class SearcherCacheManagerTest;
    friend class CachedMatchDocSearcherTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherCacheManager> SearcherCacheManagerPtr;

} // namespace search
} // namespace isearch

