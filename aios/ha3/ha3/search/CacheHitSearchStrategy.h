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
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Result.h"
#include "ha3/search/MatchDocSearchStrategy.h"
#include "indexlib/misc/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
}  // namespace partition
}  // namespace indexlib
namespace isearch {
namespace search {
struct DocCountLimits;
struct InnerSearchResult;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpressionFactory;
class JoinDocIdConverterCreator;
class SorterManager;
}  // namespace turing
}  // namespace suez

namespace autil {
namespace mem_pool {
class Pool;
};
};

namespace isearch {
namespace common {
class Request;
class SearcherCacheClause;
}  // namespace common

namespace search {

class CacheResult;
class HitCollectorManager;
class SearcherCacheManager;

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
            indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot);

    ~CacheHitSearchStrategy() override;
private:
    CacheHitSearchStrategy(const CacheHitSearchStrategy &);
    CacheHitSearchStrategy& operator=(const CacheHitSearchStrategy &);
public:
    void afterStealFromCollector(InnerSearchResult& innerResult,
                                 HitCollectorManager *hitCollectorManager) override;
private:
    common::Result *reconstructResult(common::Result *result) override;

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
    indexlib::partition::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
private:
    friend class CacheHitSearchStrategyTest;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
