#include <ha3/search/RankSearcher.h>
#include <ha3/search/ResultEstimator.h>
#include <ha3/search/LayerRangeDistributor.h>
#include <ha3/search/AggregatorCreator.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/PKQueryExecutor.h>
#include <ha3/search/LayerValidator.h>
#include <indexlib/index/partition_info.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>

using namespace std;
using namespace autil::legacy;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, RankSearcher);

RankSearcher::RankSearcher(int32_t totalDocCount,
                           autil::mem_pool::Pool *pool,
                           config::AggSamplerConfigInfo aggSamplerConfigInfo)
    : _totalDocCount(totalDocCount)
    , _pool(pool)
    , _aggSamplerConfigInfo(aggSamplerConfigInfo)
    , _filterWrapper(NULL)
    , _aggregator(NULL)
    , _timeoutTerminator(NULL)
    , _layerMetas(NULL)
    , _matchDataManager(NULL)
    , _matchCount(0)
    , _tracer(NULL)
    , _mainToSubIt(NULL)
{
}

RankSearcher::~RankSearcher() {
    POOL_DELETE_CLASS(_filterWrapper);
    DELETE_AND_SET_NULL(_aggregator);
    _timeoutTerminator = NULL;

    for (vector<QueryExecutor*>::iterator it = _queryExecutors.begin();
         it != _queryExecutors.end(); ++it)
    {
        POOL_DELETE_CLASS(*it);
    }
    _queryExecutors.clear();
}

bool RankSearcher::init(const RankSearcherParam &param)
{
    const Request *request = param.request;
    _tracer = param.tracer;
    _timeoutTerminator = param.timeoutTerminator;
    _errorResultPtr = param.errorResultPtr;
    _matchDataManager = param.matchDataManager;
    _indexPartReaderPtr = param.readerWrapper->getReader();
    _mainToSubIt = NULL;

    bool ignoreDelete = request->getConfigClause()->ignoreDelete() == ConfigClause::CB_TRUE;
    if (!ignoreDelete) {
        _delMapReader = _indexPartReaderPtr->GetDeletionMapReader();
    }
    if (request->getConfigClause()->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO) {
        _mainToSubIt = param.readerWrapper->getMainToSubIter();
        const IndexPartitionReaderPtr &subIndexPartReaderPtr = param.readerWrapper->getSubReader();
        if (subIndexPartReaderPtr && !ignoreDelete) {
            _subDelMapReader = subIndexPartReaderPtr->GetDeletionMapReader();
        }
    }
    _layerMetas = param.layerMetas;
    if (!initQueryExecutors(request, param.readerWrapper))
    {
        return false;
    }
    auto functionProvider = param.functionProvider;
    if (functionProvider) {
        functionProvider->setAttributeExprScope(AE_FILTER);
    }
    // setAttributeExprScope before function beginRequest
    if (!createFilterWrapper(request->getFilterClause(),
                             param.attrExprCreator,
                             param.matchDocAllocator))
    {
        _errorResultPtr->addError(ERROR_SEARCH_SETUP_FILTER,
                "Create filter failed");
        return false;
    }
    if (functionProvider) {
        functionProvider->setAttributeExprScope(AE_DEFAULT);
    }
    const AggregateClause *aggClause = request->getAggregateClause();
    if (aggClause) {
        _aggregator = setupAggregator(aggClause, param.attrExprCreator);
        if (NULL == _aggregator) {
            return false;
        }
    }

    return true;
}

uint32_t RankSearcher::search(RankSearcherResource &resource,
                              HitCollectorBase *hitCollector)
{
    return searchMultiLayers(resource, hitCollector, _layerMetas);
}

uint32_t RankSearcher::searchWithJoin(RankSearcherResource &resource,
                                      HashJoinInfo *hashJoinInfo,
                                      HitCollectorBase *hitCollector) {
    return searchMultiLayersWithJoin(resource, hitCollector, _layerMetas,
                                     hashJoinInfo);
}

uint32_t RankSearcher::searchMultiLayers(RankSearcherResource &resource,
        HitCollectorBase *hitCollector, LayerMetas *layerMetas)
{
    resource._sessionMetricsCollector->rankStartTrigger();
    assert(_queryExecutors.size() >= layerMetas->size());
    TimeoutTerminator *timeoutTerminator = _timeoutTerminator;
    if (timeoutTerminator) {
        timeoutTerminator->init(SEEK_CHECK_TIMEOUT_STEP);
    }
    auto matchDocAllocator = resource._matchDocAllocator;
    if (_aggregator) {
        _aggregator->setMatchDocAllocator(matchDocAllocator);
    }
    bool needScore = resource._requiredTopK != 0;
    ResultEstimator estimator;
    estimator.init(*layerMetas, _aggregator);
    LayerRangeDistributor layerRangeDistributor(layerMetas, _pool,
            _totalDocCount, resource._rankSize, _tracer);
    MatchDataManager *manager = _matchDataManager;
    bool hasTruncate = false;
    uint32_t matchCount = 0;
    uint32_t seekDocCount = 0;
    bool needFlatten = resource._needFlattenMatchDoc;
    bool needSubDoc = resource._matchDocAllocator->hasSubDocAllocator();
    while (layerRangeDistributor.hasNextLayer()) {
        uint32_t layer;
        LayerMeta *curLayer = layerRangeDistributor.getCurLayer(layer);
        QueryExecutor *queryExecutor = _queryExecutors[layer];
        double truncateChainFactor = 1.0;
        bool needAggregate = estimator.needAggregate(layer);
        SingleLayerSeekResult seekResult;
        if (queryExecutor) {
            manager->moveToLayer(layer);

            SingleLayerSearcherParam param(
                    queryExecutor, curLayer, _filterWrapper,
                    _delMapReader.get(), matchDocAllocator, timeoutTerminator,
                    _mainToSubIt, _subDelMapReader.get(), resource._getAllSubDoc);

            if (likely(needScore)) {
                seekResult = searchSingleLayerWithScore(
                        param, needSubDoc, needFlatten,
                        needAggregate ? _aggregator : NULL,
                        hitCollector, manager);
            } else {
                seekResult = searchSingleLayerWithoutScore(
                        param, needSubDoc,
                        needAggregate ? _aggregator : NULL);
            }
            truncateChainFactor = queryExecutor->getMainChainDF()
                                  / (double)queryExecutor->getCurrentDF();
            if (truncateChainFactor > 1.0) {
                hasTruncate = true;
            }
        }
        estimator.endLayer(layer, seekResult._seekCount,
                           needAggregate ? seekResult._matchCount : 0,
                           truncateChainFactor);
        matchCount += seekResult._matchCount;
        seekDocCount += seekResult._seekDocCount;
        REQUEST_TRACE(DEBUG, "layer seekCount:[ %u ], leftQuota:[ %u ], "
                      "matchCount:[ %u ], truncate ChainFactor:[ %f ],"
                      "total seekCount [ %u ], total matchCount [ %u ]",
                      seekResult._seekCount, seekResult._leftQuota,
                      seekResult._matchCount, truncateChainFactor,
                      estimator.getTotalSeekedCount(),
                      estimator.getTotalMatchCount());
        if (unlikely(seekResult._errorCode != IE_NAMESPACE(common)::ErrorCode::OK)) {
            if (seekResult._errorCode == IE_NAMESPACE(common)::ErrorCode::FileIO) {
                HA3_LOG(WARN, "seek error, indexlib error code[%d]", (int)seekResult._errorCode);
                _errorResultPtr->addError(ERROR_INDEXLIB_IO);
                break;
            }
            //  Non-FileIO ErrorCodes indicate some fatal errors,
            //    the system is hardly handle and recover.
            IE_NAMESPACE(common)::ThrowIfError(seekResult._errorCode);
        }
        // check isTimeout not isTerminated to set error code.
        if (timeoutTerminator && timeoutTerminator->isTimeout()) {
            HA3_LOG(WARN, "seek timeout, seek count [%d]", timeoutTerminator->getCheckTimes());
            _errorResultPtr->addError(ERROR_SEEKDOC_TIMEOUT);
            break;
        }
        layerRangeDistributor.moveToNextLayer(seekResult._leftQuota);
    } // end while
    hitCollector->flush();
    estimator.endSeek();
    if (_aggregator) {
        resource._sessionMetricsCollector->aggregateCountTrigger(
                _aggregator->getAggregateCount());
    }
    resource._sessionMetricsCollector->matchCountTrigger(matchCount);
    resource._sessionMetricsCollector->seekDocCountTrigger(seekDocCount);
    resource._sessionMetricsCollector->seekCountTrigger(timeoutTerminator ?
            timeoutTerminator->getCheckTimes() : estimator.getMatchCount());

    if (_filterWrapper && _filterWrapper->getJoinFilter()) {
        resource._sessionMetricsCollector->strongJoinFilterCountTrigger(
                _filterWrapper->getJoinFilter()->getFilteredCount());
    }
    if (hasTruncate) {
        resource._sessionMetricsCollector->increaseUseTruncateOptimizerNum();
    }
    _matchCount = matchCount;

    endRankPhase(_filterWrapper ? _filterWrapper->getFilter() : NULL,
                 _aggregator, hitCollector);
    REQUEST_TRACE(DEBUG, "total seekCount [ %u ], matchCount [ %u ],  "
                  "total matchCount [ %u ]", estimator.getTotalSeekedCount(),
                  estimator.getMatchCount(), estimator.getTotalMatchCount());
    return estimator.getTotalMatchCount();
}

uint32_t RankSearcher::searchMultiLayersWithJoin(
        RankSearcherResource &resource, HitCollectorBase *hitCollector,
        LayerMetas *layerMetas, HashJoinInfo *hashJoinInfo)
{
    resource._sessionMetricsCollector->rankStartTrigger();
    assert(_queryExecutors.size() >= layerMetas->size());
    TimeoutTerminator *timeoutTerminator = _timeoutTerminator;
    if (timeoutTerminator) {
        timeoutTerminator->init(SEEK_CHECK_TIMEOUT_STEP);
    }
    auto matchDocAllocator = resource._matchDocAllocator;
    if (_aggregator) {
        _aggregator->setMatchDocAllocator(matchDocAllocator);
    }
    bool needScore = resource._requiredTopK != 0;
    ResultEstimator estimator;
    estimator.init(*layerMetas, _aggregator);
    LayerRangeDistributor layerRangeDistributor(layerMetas, _pool,
            _totalDocCount, resource._rankSize, _tracer);
    MatchDataManager *manager = _matchDataManager;
    bool hasTruncate = false;
    uint32_t matchCount = 0;
    uint32_t seekDocCount = 0;
    bool needFlatten = resource._needFlattenMatchDoc;
    bool needSubDoc = resource._matchDocAllocator->hasSubDocAllocator();
    while (layerRangeDistributor.hasNextLayer()) {
        uint32_t layer;
        LayerMeta *curLayer = layerRangeDistributor.getCurLayer(layer);
        QueryExecutor *queryExecutor = _queryExecutors[layer];
        double truncateChainFactor = 1.0;
        bool needAggregate = estimator.needAggregate(layer);
        SingleLayerSeekResult seekResult;
        if (queryExecutor) {
            manager->moveToLayer(layer);

            SingleLayerSearcherParam param(
                    queryExecutor, curLayer, _filterWrapper,
                    _delMapReader.get(), matchDocAllocator, timeoutTerminator,
                    _mainToSubIt, _subDelMapReader.get(), resource._getAllSubDoc);

            if (likely(needScore)) {
                seekResult = searchSingleLayerWithScoreAndJoin(
                        param, needSubDoc, needFlatten,
                        needAggregate ? _aggregator : NULL,
                        hitCollector, manager, hashJoinInfo);
            } else {
                seekResult = searchSingleLayerJoinWithoutScore(param,
                        needSubDoc, needAggregate ? _aggregator : NULL,
                        hashJoinInfo);
            }
            truncateChainFactor = queryExecutor->getMainChainDF()
                                  / (double)queryExecutor->getCurrentDF();
            if (truncateChainFactor > 1.0) {
                hasTruncate = true;
            }
        }
        estimator.endLayer(layer, seekResult._seekCount,
                           needAggregate ? seekResult._matchCount : 0,
                           truncateChainFactor);
        matchCount += seekResult._matchCount;
        seekDocCount += seekResult._seekDocCount;
        REQUEST_TRACE(DEBUG, "layer seekCount:[ %u ], leftQuota:[ %u ], "
                      "matchCount:[ %u ], truncate ChainFactor:[ %f ],"
                      "total seekCount [ %u ], total matchCount [ %u ]",
                      seekResult._seekCount, seekResult._leftQuota,
                      seekResult._matchCount, truncateChainFactor,
                      estimator.getTotalSeekedCount(),
                      estimator.getTotalMatchCount());
        if (unlikely(seekResult._errorCode != IE_NAMESPACE(common)::ErrorCode::OK)) {
            if (seekResult._errorCode == IE_NAMESPACE(common)::ErrorCode::FileIO) {
                HA3_LOG(WARN, "seek error, indexlib error code[%d]", (int)seekResult._errorCode);
                _errorResultPtr->addError(ERROR_INDEXLIB_IO);
                break;
            }
            //  Non-FileIO ErrorCodes indicate some fatal errors,
            //    the system is hardly handle and recover.
            IE_NAMESPACE(common)::ThrowIfError(seekResult._errorCode);
        }
        // check isTimeout not isTerminated to set error code.
        if (timeoutTerminator && timeoutTerminator->isTimeout()) {
            HA3_LOG(WARN, "seek timeout, seek count [%d]", timeoutTerminator->getCheckTimes());
            _errorResultPtr->addError(ERROR_SEEKDOC_TIMEOUT);
            break;
        }
        layerRangeDistributor.moveToNextLayer(seekResult._leftQuota);
    } // end while
    hitCollector->flush();
    estimator.endSeek();
    if (_aggregator) {
        resource._sessionMetricsCollector->aggregateCountTrigger(
                _aggregator->getAggregateCount());
    }
    resource._sessionMetricsCollector->matchCountTrigger(matchCount);
    resource._sessionMetricsCollector->seekDocCountTrigger(seekDocCount);
    resource._sessionMetricsCollector->seekCountTrigger(timeoutTerminator ?
            timeoutTerminator->getCheckTimes() : estimator.getMatchCount());

    if (_filterWrapper && _filterWrapper->getJoinFilter()) {
        resource._sessionMetricsCollector->strongJoinFilterCountTrigger(
                _filterWrapper->getJoinFilter()->getFilteredCount());
    }
    if (hasTruncate) {
        resource._sessionMetricsCollector->increaseUseTruncateOptimizerNum();
    }
    _matchCount = matchCount;

    endRankPhase(_filterWrapper ? _filterWrapper->getFilter() : NULL,
                 _aggregator, hitCollector);
    REQUEST_TRACE(DEBUG, "total seekCount [ %u ], matchCount [ %u ],  "
                  "total matchCount [ %u ]", estimator.getTotalSeekedCount(),
                  estimator.getMatchCount(), estimator.getTotalMatchCount());
    return estimator.getTotalMatchCount();
}

SingleLayerSeekResult RankSearcher::searchSingleLayerWithoutScore(
        const SingleLayerSearcherParam &param,
        bool needSubDoc, Aggregator *aggregator)
{
    SingleLayerSeekResult result;

    SingleLayerSearcher singleLayerSearcher(param._queryExecutor,
            param._layerMeta, param._filterWrapper, param._deletionMapReader,
            param._matchDocAllocator, param._timeoutTerminator,
            param._main2SubIt, param._subDeletionMapReader, NULL, param._getAllSubDoc);

    auto matchDocAllocator = param._matchDocAllocator;
    auto ec = IE_NAMESPACE(common)::ErrorCode::OK;
    while (true) {
        matchdoc::MatchDoc matchDoc;
        ec = singleLayerSearcher.seek(needSubDoc, matchDoc);
        if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
            break;
        }
        if (matchdoc::INVALID_MATCHDOC == matchDoc) {
            break;
        }
        if (aggregator) {
            aggregator->aggregate(matchDoc);
        }
        ++result._matchCount;
        matchDocAllocator->deallocate(matchDoc);
    }

    result._seekCount = singleLayerSearcher.getSeekedCount();
    result._leftQuota = singleLayerSearcher.getLeftQuotaInTheEnd();
    result._seekDocCount = singleLayerSearcher.getSeekDocCount();
    result._errorCode = ec;
    return result;
}

SingleLayerSeekResult RankSearcher::searchSingleLayerWithScore(
        const SingleLayerSearcherParam &param,
        bool needSubDoc, bool needFlatten,
        Aggregator *aggregator,
        HitCollectorBase *hitCollector,
        MatchDataManager *manager)
{
    SingleLayerSearcher singleLayerSearcher(param._queryExecutor,
            param._layerMeta, param._filterWrapper, param._deletionMapReader,
            param._matchDocAllocator, param._timeoutTerminator,
            param._main2SubIt, param._subDeletionMapReader, manager, param._getAllSubDoc);
    auto ec = IE_NAMESPACE(common)::ErrorCode::OK;
    while (true) {
        matchdoc::MatchDoc matchDoc;
        ec = singleLayerSearcher.seek(needSubDoc, matchDoc);
        if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
            break;
        }
        if (matchdoc::INVALID_MATCHDOC == matchDoc) {
            break;
        }
        if (aggregator) {
            aggregator->aggregate(matchDoc);
        }
        hitCollector->collect(matchDoc, needFlatten);
    }

    SingleLayerSeekResult result;
    result._matchCount = hitCollector->stealCollectCount();
    result._seekCount = singleLayerSearcher.getSeekedCount();
    result._leftQuota = singleLayerSearcher.getLeftQuotaInTheEnd();
    result._seekDocCount = singleLayerSearcher.getSeekDocCount();
    result._errorCode = ec;
    return result;
}

SingleLayerSeekResult RankSearcher::searchSingleLayerJoinWithoutScore(
        const SingleLayerSearcherParam &param,
        bool needSubDoc,
        Aggregator *aggregator,
        common::HashJoinInfo *hashJoinInfo) {
    auto *joinAttrExpr = hashJoinInfo->getJoinAttrExpr();
    switch (joinAttrExpr->getType()) {

#define CASE_MACRO(vt_type)                                                    \
    case vt_type: {                                                            \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;            \
        auto *joinAttrExprTyped =                                              \
                dynamic_cast<AttributeExpressionTyped<T> *>(joinAttrExpr);     \
        assert(joinAttrExprTyped);                                             \
        return doSearchSingleLayerJoinWithoutScore<T>(param, needSubDoc,       \
                aggregator, hashJoinInfo, joinAttrExprTyped);                  \
        break;                                                                 \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
	assert(0);
    }

    }
    return {};
}

SingleLayerSeekResult RankSearcher::searchSingleLayerWithScoreAndJoin(
        const SingleLayerSearcherParam &param, bool needSubDoc,
        bool needFlatten, Aggregator *aggregator,
        HitCollectorBase *hitCollector, MatchDataManager *manager,
        HashJoinInfo *hashJoinInfo)
{
    auto *joinAttrExpr = hashJoinInfo->getJoinAttrExpr();
    switch (joinAttrExpr->getType()) {

#define CASE_MACRO(vt_type)                                                    \
    case vt_type: {                                                            \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;            \
        auto *joinAttrExprTyped =                                              \
                dynamic_cast<AttributeExpressionTyped<T> *>(joinAttrExpr);     \
        assert(joinAttrExprTyped);                                             \
        return doSearchSingleLayerWithScoreAndJoin<T>(param, needSubDoc,       \
                needFlatten, aggregator, hitCollector, manager, hashJoinInfo,  \
                joinAttrExprTyped);                                            \
        break;                                                                 \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
	assert(0);
    }

    }
    return {};
}


void RankSearcher::endRankPhase(Filter *filter,
                                Aggregator *aggregator,
                                HitCollectorBase *hitCollector)
{
    if (filter) {
        filter->updateExprEvaluatedStatus();
    }
    if (aggregator) {
        aggregator->updateExprEvaluatedStatus();
    }
    if (hitCollector) {
        hitCollector->updateExprEvaluatedStatus();
    }
}

Aggregator *RankSearcher::setupAggregator(const AggregateClause *aggClause,
        AttributeExpressionCreator *attrExprCreator)
{
    assert(aggClause);

    AggregatorCreator aggCreator(attrExprCreator, _pool, NULL, _tracer);
    aggCreator.setAggSamplerConfigInfo(_aggSamplerConfigInfo);
    Aggregator *aggregator = aggCreator.createAggregator(aggClause);
    if (NULL == aggregator) {
        string errorStr = "create aggregator failed";
        HA3_LOG(WARN, "%s", errorStr.c_str());
        _errorResultPtr->addError(ERROR_SEARCH_SETUP_AGGREGATOR,
                errorStr.c_str());
    }
    return aggregator;
}

bool RankSearcher::initQueryExecutors(const Request *request,
                                      IndexPartitionReaderWrapper* wrapper)
{
    uint32_t layerCount = _layerMetas->size();
    const QueryClause *queryClause = request->getQueryClause();
    uint32_t queryCount = queryClause->getQueryCount();
    uint32_t needQueryCount = min(queryCount, layerCount);
    _matchDataManager->setQueryCount(needQueryCount);
    _queryExecutors.resize(layerCount);
    uint32_t i = 0;
    const PKFilterClause *pkFilterClause = request->getPKFilterClause();
    const Query *query = NULL;
    bool succOnce = false;
    // set need clone : same term in query should be different posting iterator.
    for (; i < needQueryCount; ++i) {
        query = queryClause->getRootQuery(i);
        _queryExecutors[i] = createQueryExecutor(query, wrapper, pkFilterClause, &(*_layerMetas)[i]);
        if (_queryExecutors[i]) {
            succOnce = true;
        }
    }
    Query *firstQuery = queryCount > 0 ? queryClause->getRootQuery(0) : NULL;

    // no query specified, use first query for the rest layers.
    for (; i < layerCount; ++i) {
        _queryExecutors[i] = createQueryExecutor(firstQuery, wrapper, pkFilterClause, &(*_layerMetas)[i]);
        if (_queryExecutors[i]) {
            succOnce = true;
        }
    }

    REQUEST_TRACE(TRACE3, "timeout check after lookup");
    if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
        HA3_LOG(WARN, "lookup timeout");
        _errorResultPtr->addError(ERROR_LOOKUP_TIMEOUT);
    }

    return succOnce && _errorResultPtr->getErrorCount() == 0;
}

QueryExecutor* RankSearcher::createQueryExecutor(const Query *query,
        IndexPartitionReaderWrapper *wrapper,
        const PKFilterClause *pkFilterClause,
        const LayerMeta *layerMeta)
{
    QueryExecutor *queryExecutor = NULL;
    ErrorCode errorCode = ERROR_NONE;
    std::string errorMsg;

    try {
        HA3_LOG(DEBUG, "query:[%p]", query);
        QueryExecutorCreator qeCreator(_matchDataManager, wrapper, _pool,
                _timeoutTerminator, layerMeta);
        query->accept(&qeCreator);
        queryExecutor = qeCreator.stealQuery();
        if (queryExecutor->isEmpty()) {
            POOL_DELETE_CLASS(queryExecutor);
            queryExecutor = NULL;
        }
        if (pkFilterClause) {
            QueryExecutor *pkExecutor = createPKExecutor(
                    pkFilterClause, queryExecutor);
            queryExecutor = pkExecutor;
        }
    } catch (const ExceptionBase &e) {
        HA3_LOG(WARN, "lookup exception: %s", e.what());
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = ERROR_SEARCH_LOOKUP;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_LOOKUP_FILEIO_ERROR;
        }
    } catch (const std::exception& e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_LOOKUP;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_LOOKUP;
    }

    if (errorCode != ERROR_NONE) {
        HA3_LOG(WARN, "Exception: [%s]", errorMsg.c_str());
        HA3_LOG(WARN, "Create query executor failed, query [%s]",
                query->toString().c_str());
        _errorResultPtr->addError(errorCode, errorMsg);
    }
    return queryExecutor;
}

QueryExecutor* RankSearcher::createPKExecutor(
        const PKFilterClause *pkFilterClause, QueryExecutor *queryExecutor)
{
    assert(pkFilterClause);
    const IndexReaderPtr &primaryKeyReaderPtr
        = _indexPartReaderPtr->GetPrimaryKeyReader();
    if (!primaryKeyReaderPtr) {
        HA3_LOG(WARN, "pkFilterClause is NULL");
        return NULL;
    }
    IE_NAMESPACE(util)::Term term(pkFilterClause->getOriginalString(), "");
    IE_NAMESPACE(index)::PostingIterator *iter = primaryKeyReaderPtr->Lookup(term, 1, pt_default, _pool);
    docid_t docId = iter ? iter->SeekDoc(0) : END_DOCID;
    QueryExecutor *pkExecutor = POOL_NEW_CLASS(_pool,
            PKQueryExecutor, queryExecutor, docId);
    POOL_DELETE_CLASS(iter);
    return pkExecutor;
}

bool RankSearcher::createFilterWrapper(
        const FilterClause *filterClause,
        AttributeExpressionCreator *attrExprCreator,
        common::Ha3MatchDocAllocator *matchDocAllocator)
{
    Filter *filter = NULL;
    SubDocFilter *subDocFilter = NULL;
    if (filterClause) {
        filter = Filter::createFilter(filterClause, attrExprCreator, _pool);
        if (filter == NULL) {
            HA3_LOG(WARN, "Create Filter failed.");
            return false;
        }
    }
    if (matchDocAllocator->hasSubDocAllocator()) {
        subDocFilter = POOL_NEW_CLASS(_pool, SubDocFilter,
                matchDocAllocator->getSubDocAccessor());
    }
    if (filter != NULL || subDocFilter != NULL) {
        _filterWrapper = POOL_NEW_CLASS(_pool, FilterWrapper);
        _filterWrapper->setFilter(filter);
        _filterWrapper->setSubDocFilter(subDocFilter);
    }
    return true;
}

void RankSearcher::createJoinFilter(
        JoinDocIdConverterCreator *docIdConverterFactory, JoinType joinType)
{
    if (!docIdConverterFactory || joinType == WEAK_JOIN) {
        return;
    }
    bool forceStrongJoin = joinType == STRONG_JOIN;
    bool hasStrongJoin = docIdConverterFactory->hasStrongJoinConverter();
    if (forceStrongJoin || hasStrongJoin) {
        JoinFilter *joinFilter = POOL_NEW_CLASS(_pool, JoinFilter,
                docIdConverterFactory, forceStrongJoin);
        if (_filterWrapper == NULL) {
            _filterWrapper = POOL_NEW_CLASS(_pool, FilterWrapper);
        }
        _filterWrapper->setJoinFilter(joinFilter);
    }
}

END_HA3_NAMESPACE(search);
