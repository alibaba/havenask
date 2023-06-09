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
#include "ha3/search/CacheHitSearchStrategy.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/FinalSortClause.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proxy/Merger.h"
#include "ha3/search/CacheMinScoreFilter.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/HitCollectorManager.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearchStrategy.h"
#include "ha3/search/SearcherCache.h"
#include "ha3/search/SearcherCacheManager.h"
#include "ha3/search/SortExpression.h"
#include "ha3/sorter/SorterResource.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionFactory.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/framework/JoinDocIdConverterCreator.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/plugin/SorterWrapper.h"

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
}  // namespace partition
}  // namespace indexlib
namespace isearch {
namespace rank {
class HitCollectorBase;
}  // namespace rank
}  // namespace isearch

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::monitor;
using namespace isearch::proxy;
using namespace isearch::rank;
using namespace isearch::sorter;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, CacheHitSearchStrategy);

CacheHitSearchStrategy::CacheHitSearchStrategy(
        const DocCountLimits& docCountLimits,
        const Request *request,
        const SearcherCacheManager *cacheManager,
        const SorterManager *sorterManager,
        autil::mem_pool::Pool *pool,
        std::string mainTable,
        indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot)
    : MatchDocSearchStrategy(docCountLimits)
    , _request(request)
    , _cacheManager(cacheManager)
    , _sorterManager(sorterManager)
    , _pool(pool)
    , _mainTable(mainTable)
    , _partitionReaderSnapshot(partitionReaderSnapshot)
{
}

CacheHitSearchStrategy::~CacheHitSearchStrategy() { }

void CacheHitSearchStrategy::afterStealFromCollector(InnerSearchResult& innerResult, HitCollectorManager *hitCollectorManager)
{
    SessionMetricsCollector *collector = _cacheManager->getMetricsCollector();
    collector->increaseCacheHitNum();

    auto searcherCache = _cacheManager->getSearcherCache();
    searcherCache->increaseCacheHitNum();

    CacheResult *cacheResult = _cacheManager->getCacheResult();

    AUTIL_LOG(DEBUG, "seek from inc, totalMatchDocs:[%d], "
            "matchDocCount:[%zd]",
            innerResult.totalMatchDocs, innerResult.matchDocVec.size());

    if (searcherCache->exceedIncDocLimit(innerResult.totalMatchDocs)) {
        common::SearcherCacheClause *cacheClause = _request->getSearcherCacheClause();
        assert(cacheClause);
        uint64_t key = cacheClause->getKey();
        PartitionRange partRange = cacheClause->getPartRange();
        searcherCache->deleteCacheItem(key, partRange);
    }

    if (innerResult.matchDocVec.size() > 0) {
        size_t expectCount = getExpectCount(_docCountLimits.runtimeTopK,
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

common::Result *CacheHitSearchStrategy::reconstructResult(common::Result *result) {
    CacheResult *cacheResult = _cacheManager->getCacheResult();
    refreshAttributes(cacheResult->getResult());
    return mergeResult(cacheResult->stealResult(),
                       result, _request, _docCountLimits.requiredTopK);
}

void CacheHitSearchStrategy::refreshAttributes(common::Result *result) {
    MatchDocs *matchDocs = result->getMatchDocs();
    if (!matchDocs || matchDocs->size() == 0) {
        return;
    }
    ExpressionVector needRefreshAttrs;

    const auto &allocator = matchDocs->getMatchDocAllocator();
    JoinDocIdConverterCreator *docIdConverterFactory = POOL_NEW_CLASS(
            _pool, JoinDocIdConverterCreator, _pool, allocator.get());
    AttributeExpressionFactory *attrExprFactory = POOL_NEW_CLASS(
            _pool, AttributeExpressionFactory,
            _mainTable,
            _partitionReaderSnapshot,
            docIdConverterFactory,
            _pool, allocator.get());
    initRefreshAttrs(allocator, docIdConverterFactory, attrExprFactory,
                     _request->getSearcherCacheClause(), needRefreshAttrs);
    if (needRefreshAttrs.size() == 0) {
        POOL_DELETE_CLASS(attrExprFactory);
        POOL_DELETE_CLASS(docIdConverterFactory);
        return;
    }
    allocator->extend();
    ExpressionEvaluator<ExpressionVector> evaluator(
            needRefreshAttrs, allocator->getSubDocAccessor());
    auto &matchDocVec = matchDocs->getMatchDocsVect();
    evaluator.batchEvaluateExpressions(&matchDocVec[0], matchDocVec.size());
    for (size_t i = 0; i < needRefreshAttrs.size(); i++) {
        POOL_DELETE_CLASS(needRefreshAttrs[i]);
    }

    POOL_DELETE_CLASS(attrExprFactory);
    POOL_DELETE_CLASS(docIdConverterFactory);
}

void CacheHitSearchStrategy::initRefreshAttrs(
        const common::Ha3MatchDocAllocatorPtr &vsa,
        JoinDocIdConverterCreator *docIdConverterFactory,
        AttributeExpressionFactory *attrExprFactory,
        SearcherCacheClause *searcherCacheClause,
        ExpressionVector &needRefreshAttrs)
{
    assert(searcherCacheClause != NULL);
    const set<string> &refreshAttrNames =
        searcherCacheClause->getRefreshAttributes();
    vector<string> needRefreshAttrNames;
    for (set<string>::const_iterator it = refreshAttrNames.begin();
         it != refreshAttrNames.end(); it++)
    {
        if (vsa->findReferenceWithoutType(*it) != NULL) {
            needRefreshAttrNames.push_back(*it);
        }
    }
    if (needRefreshAttrNames.size() == 0) {
        return;
    }
    createNeedRefreshAttrExprs(vsa, docIdConverterFactory, attrExprFactory,
                               needRefreshAttrNames, needRefreshAttrs);
}

void CacheHitSearchStrategy::createNeedRefreshAttrExprs(
        const common::Ha3MatchDocAllocatorPtr &allocator,
        JoinDocIdConverterCreator *docIdConverterFactory,
        AttributeExpressionFactory *attrExprFactory,
        const vector<string> &needRefreshAttrNames,
        ExpressionVector &needRefreshAttrs)
{
    for (size_t i = 0; i < needRefreshAttrNames.size(); i++) {
        const string &attrName = needRefreshAttrNames[i];
        auto refBase = allocator->findReferenceWithoutType(attrName);
        AttributeExpression *expr = attrExprFactory->createAtomicExpr(attrName);
        if (!expr) {
            AUTIL_LOG(WARN, "failed to createAtomicExpr for %s", attrName.c_str());
            continue;
        }
        auto valueType = refBase->getValueType();
        auto vt = valueType.getBuiltinType();
        bool isMulti = valueType.isMultiValue();
#define ATTR_TYPE_CASE_HELPER(vt_type)                                  \
        case vt_type:                                                   \
            if (isMulti) {                                              \
                typedef VariableTypeTraits<vt_type, true>::AttrExprType AttrExprType; \
                AttributeExpressionTyped<AttrExprType> *typedExpr       \
                    = dynamic_cast<AttributeExpressionTyped<AttrExprType> *>(expr); \
                assert(typedExpr != NULL);                              \
                auto ref = dynamic_cast<matchdoc::Reference<AttrExprType> *>(refBase); \
                typedExpr->setReference(ref);                           \
            } else {                                                    \
                typedef VariableTypeTraits<vt_type, false>::AttrExprType AttrExprType; \
                AttributeExpressionTyped<AttrExprType> *typedExpr       \
                    = dynamic_cast<AttributeExpressionTyped<AttrExprType> *>(expr); \
                assert(typedExpr != NULL);                              \
                auto ref = dynamic_cast<matchdoc::Reference<AttrExprType> *>(refBase); \
                typedExpr->setReference(ref);                           \
            }                                                           \
            needRefreshAttrs.push_back(expr);                           \
            break

        switch (vt) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER(ATTR_TYPE_CASE_HELPER);
            ATTR_TYPE_CASE_HELPER(vt_string);
            ATTR_TYPE_CASE_HELPER(vt_hash_128);
        default:
            break;
        }
#undef ATTR_TYPE_CASE_HELPER
    }
}

size_t CacheHitSearchStrategy::getExpectCount(uint32_t topK,
        CacheResult *cacheResult)
{
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

common::Result *CacheHitSearchStrategy::mergeResult(common::Result *fullResult,
                                                    common::Result *incResult,
                                                    const Request *request,
                                                    uint32_t requireTopK) {
    ResultPtrVector results;
    // first result allocator merge other allocators
    // fullResult first
    results.push_back(ResultPtr(fullResult));
    results.push_back(ResultPtr(incResult));

    common::Result *result = new common::Result;
    std::string sorterName = "DEFAULT";
    auto finalSortClause = request->getFinalSortClause();
    if (finalSortClause) {
        sorterName = finalSortClause->getSortName();
    }
    auto sorter = _sorterManager->getSorter(sorterName);
    if (!sorter) {
        result->addErrorResult(ErrorResult(ERROR_CREATE_SORTER));
        return result;
    }
    SorterWrapper sorterWrapper(sorter->clone());
    Merger::mergeResults(result, results, request, &sorterWrapper,
                         sorter::SL_SEARCHCACHEMERGER);
    result->setUseTruncateOptimizer(fullResult->useTruncateOptimizer());
    return result;
}

} // namespace search
} // namespace isearch
