#include <ha3/search/CacheHitSearchStrategy.h>
#include <ha3/search/SearcherCacheManager.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <suez/turing/expression/framework/AttributeExpressionFactory.h>
#include <ha3/search/HitCollectorManager.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/proxy/Merger.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(proxy);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(sorter);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, CacheHitSearchStrategy);

CacheHitSearchStrategy::CacheHitSearchStrategy(
        const DocCountLimits& docCountLimits,
        const Request *request,
        const SearcherCacheManager *cacheManager,
        const SorterManager *sorterManager,
        autil::mem_pool::Pool *pool,
        std::string mainTable,
        IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot)
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

    HA3_LOG(DEBUG, "seek from inc, totalMatchDocs:[%d], "
            "matchDocCount:[%zd]",
            innerResult.totalMatchDocs, innerResult.matchDocVec.size());

    if (searcherCache->exceedIncDocLimit(innerResult.totalMatchDocs)) {
        searcherCache->deleteCacheItem(getKey(_request));
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

Result* CacheHitSearchStrategy::reconstructResult(Result* result) {
    CacheResult *cacheResult = _cacheManager->getCacheResult();
    refreshAttributes(cacheResult->getResult());
    return mergeResult(cacheResult->stealResult(),
                       result, _request, _docCountLimits.requiredTopK);
}

void CacheHitSearchStrategy::refreshAttributes(Result *result)
{
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
            HA3_LOG(WARN, "failed to createAtomicExpr for %s", attrName.c_str());
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
    Result *result = cacheResult->getResult();
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

Result* CacheHitSearchStrategy::mergeResult(
        Result *fullResult, Result *incResult,
        const Request *request, uint32_t requireTopK)
{
    ResultPtrVector results;
    // first result allocator merge other allocators
    // fullResult first
    results.push_back(ResultPtr(fullResult));
    results.push_back(ResultPtr(incResult));

    Result *result = new Result;
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

END_HA3_NAMESPACE(search);
