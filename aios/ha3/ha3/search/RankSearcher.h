#ifndef ISEARCH_RANKSEARCHER_H
#define ISEARCH_RANKSEARCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/Filter.h>
#include <ha3/search/Aggregator.h>
#include <ha3/common/MultiErrorResult.h>
#include <ha3/common/Request.h>
#include <indexlib/partition/index_partition_reader.h>
#include <indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h>
#include <ha3/rank/GlobalMatchData.h>
#include <ha3/rank/HitCollectorBase.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/config/AggSamplerConfigInfo.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <suez/turing/expression/function/FunctionManager.h>
#include <ha3/search/SingleLayerSearcher.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/func_expression/FunctionProvider.h>
#include <ha3/common/HashJoinInfo.h>

BEGIN_HA3_NAMESPACE(search);

struct RankSearcherResource {
    RankSearcherResource() {
        _needFlattenMatchDoc = false;
        _requiredTopK = 0;
        _rankSize = 0;
        _matchDocAllocator = NULL;
        _sessionMetricsCollector = NULL;
        _getAllSubDoc = false;
    }
    bool _needFlattenMatchDoc;
    uint32_t _requiredTopK;
    uint32_t _rankSize;
    monitor::SessionMetricsCollector *_sessionMetricsCollector;
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    bool _getAllSubDoc;
};

struct RankSearcherParam {
public:
    RankSearcherParam()
        : request(NULL)
        , attrExprCreator(NULL)
        , matchDataManager(NULL)
        , readerWrapper(NULL)
        , rankSize(0)
        , timeoutTerminator(NULL)
        , tracer(NULL)
        , matchDocAllocator(NULL)
        , functionProvider(NULL)
        , layerMetas(NULL)
    {}
public:
    const common::Request *request;
    suez::turing::AttributeExpressionCreator *attrExprCreator;
    MatchDataManager *matchDataManager;
    IndexPartitionReaderWrapper *readerWrapper;
    uint32_t rankSize;
    common::TimeoutTerminator *timeoutTerminator;
    common::Tracer *tracer;
    common::MultiErrorResultPtr errorResultPtr;
    common::Ha3MatchDocAllocator *matchDocAllocator;
    func_expression::FunctionProvider *functionProvider;
    LayerMetas *layerMetas;
};

struct SingleLayerSearcherParam {
    SingleLayerSearcherParam(
            QueryExecutor *queryExecutor,
            LayerMeta *layerMeta,
            FilterWrapper *filterWrapper,
            IE_NAMESPACE(index)::DeletionMapReader *deletionMapReader,
            common::Ha3MatchDocAllocator *matchDocAllocator,
            common::TimeoutTerminator *timeoutTerminator,
            IE_NAMESPACE(index)::JoinDocidAttributeIterator *main2SubIt,
            IE_NAMESPACE(index)::DeletionMapReader *subDeletionMapReader,
            bool getAllSubDoc)
        : _queryExecutor(queryExecutor)
        , _layerMeta(layerMeta)
        , _filterWrapper(filterWrapper)
        , _matchDocAllocator(matchDocAllocator)
        , _timeoutTerminator(timeoutTerminator)
        , _main2SubIt(main2SubIt)
        , _deletionMapReader(deletionMapReader)
        , _subDeletionMapReader(subDeletionMapReader)
        , _getAllSubDoc(getAllSubDoc)
    {
    }
public:
    QueryExecutor *_queryExecutor;
    LayerMeta *_layerMeta;
    FilterWrapper *_filterWrapper;
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    common::TimeoutTerminator *_timeoutTerminator;
    IE_NAMESPACE(index)::JoinDocidAttributeIterator *_main2SubIt;
    IE_NAMESPACE(index)::DeletionMapReader *_deletionMapReader;
    IE_NAMESPACE(index)::DeletionMapReader *_subDeletionMapReader;
    bool _getAllSubDoc;
};

struct SingleLayerSeekResult {
    SingleLayerSeekResult()
        : _seekCount(0)
        , _matchCount(0)
        , _leftQuota(0)
        , _seekDocCount(0)
        , _errorCode(IE_NAMESPACE(common)::ErrorCode::OK)
    {}
public:
    uint32_t _seekCount;
    uint32_t _matchCount;
    uint32_t _leftQuota;
    uint32_t _seekDocCount;
    IE_NAMESPACE(common)::ErrorCode _errorCode;
};

class RankSearcher
{
public:
    typedef IE_NAMESPACE(index)::JoinDocidAttributeIterator DocMapAttrIterator;

public:
    RankSearcher(int32_t totalDocCount,
                 autil::mem_pool::Pool *pool,
                 config::AggSamplerConfigInfo aggSamplerConfigInfo);
    ~RankSearcher();
private:
    RankSearcher(const RankSearcher &);
    RankSearcher& operator=(const RankSearcher &);
public:
    bool init(const RankSearcherParam &param);


    void createJoinFilter(suez::turing::JoinDocIdConverterCreator *docIdConverterFactory,
                          JoinType joinType);

    uint32_t search(RankSearcherResource &resource,
                    rank::HitCollectorBase *hitCollector);
    uint32_t searchWithJoin(RankSearcherResource &resource,
                            common::HashJoinInfo *hashJoinInfo,
                            rank::HitCollectorBase *hitCollector);

    void collectAggregateResults(common::AggregateResultsPtr &aggResultsPtr) {
        if (_aggregator) {
            aggResultsPtr = _aggregator->collectAggregateResult();
        }
    }
    bool hasBatchAgg() const {
        return _aggregator && _aggSamplerConfigInfo.getAggBatchMode();
    }

    const std::vector<QueryExecutor*> &getQueryExecutors() const {
        return _queryExecutors;
    }

    uint32_t getMatchCount() {
        return _matchCount;
    }
private:
    Aggregator* setupAggregator(const common::AggregateClause *aggClause,
                                suez::turing::AttributeExpressionCreator *attrExprCreator);

    bool initQueryExecutors(const common::Request *request,
                            IndexPartitionReaderWrapper *wrapper);

    SingleLayerSeekResult searchSingleLayerWithoutScore(
            const SingleLayerSearcherParam &param,
            bool needSubDoc, Aggregator *aggregator)__attribute__((noinline));

    SingleLayerSeekResult searchSingleLayerWithScore(
            const SingleLayerSearcherParam &param,
            bool needSubDoc, bool needFlatten,
            Aggregator *aggregator,
            rank::HitCollectorBase *hitCollector,
            MatchDataManager *manager)__attribute__((noinline));

    SingleLayerSeekResult searchSingleLayerWithScoreAndJoin(
            const SingleLayerSearcherParam &param,
            bool needSubDoc, bool needFlatten,
            Aggregator *aggregator,
            rank::HitCollectorBase *hitCollector,
            MatchDataManager *manager,
	    common::HashJoinInfo *hashJoinInfo
	)__attribute__((noinline));

    template <typename T>
    SingleLayerSeekResult doSearchSingleLayerWithScoreAndJoin(
            const SingleLayerSearcherParam &param,
            bool needSubDoc,
            bool needFlatten,
            Aggregator *aggregator,
            rank::HitCollectorBase *hitCollector,
            MatchDataManager *manager,
            common::HashJoinInfo *hashJoinInfo,
            suez::turing::AttributeExpressionTyped<T> *joinAttrExpr)
            __attribute__((noinline));

    SingleLayerSeekResult searchSingleLayerJoinWithoutScore(
            const SingleLayerSearcherParam &param,
            bool needSubDoc,
            Aggregator *aggregator,
            common::HashJoinInfo *hashJoinInfo)
            __attribute__((noinline));

    template <typename T>
    SingleLayerSeekResult doSearchSingleLayerJoinWithoutScore(
            const SingleLayerSearcherParam &param,
            bool needSubDoc,
            Aggregator *aggregator,
            common::HashJoinInfo *hashJoinInfo,
            suez::turing::AttributeExpressionTyped<T> *joinAttrExpr)
            __attribute__((noinline));

    void endRankPhase(Filter *filter, Aggregator *aggregator,
                      rank::HitCollectorBase *hitCollector);
private:
    QueryExecutor* createQueryExecutor(const common::Query *query,
            IndexPartitionReaderWrapper *wrapper,
            const common::PKFilterClause *pkFilterClause,
            const LayerMeta *layerMeta);

    QueryExecutor* createPKExecutor(
            const common::PKFilterClause *pkFilterClause,
            QueryExecutor *queryExecutor);
private:
    uint32_t searchMultiLayers(RankSearcherResource &resource,
                               rank::HitCollectorBase *hitCollector,
                               LayerMetas *layerMetas);
    uint32_t
    searchMultiLayersWithJoin(RankSearcherResource &resource,
                              rank::HitCollectorBase *hitCollector,
                              LayerMetas *layerMetas,
                              common::HashJoinInfo *hashJoinInfo);

    bool createFilterWrapper(const common::FilterClause *filterClause,
                             suez::turing::AttributeExpressionCreator *attrExprCreator,
                             common::Ha3MatchDocAllocator *matchDocAllocator);
private:
    static const int32_t SEEK_CHECK_TIMEOUT_STEP = 1 << 10;
private:
    friend class RankSearcherTest;
private:
    int32_t _totalDocCount;
    autil::mem_pool::Pool *_pool;
    config::AggSamplerConfigInfo _aggSamplerConfigInfo;
    IE_NAMESPACE(partition)::IndexPartitionReaderPtr _indexPartReaderPtr;
    std::vector<QueryExecutor *> _queryExecutors;
    FilterWrapper *_filterWrapper;
    Aggregator *_aggregator;
    common::TimeoutTerminator *_timeoutTerminator;
    common::MultiErrorResultPtr _errorResultPtr;
    LayerMetas *_layerMetas;
    MatchDataManager *_matchDataManager;
    uint32_t _matchCount;
    common::Tracer *_tracer;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _delMapReader;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _subDelMapReader;
    DocMapAttrIterator *_mainToSubIt;
private:
    friend class MatchDocSearcherTest;
private:
    HA3_LOG_DECLARE();
};

template <typename T>
SingleLayerSeekResult RankSearcher::doSearchSingleLayerJoinWithoutScore(
        const SingleLayerSearcherParam &param,
        bool needSubDoc,
        Aggregator *aggregator,
        common::HashJoinInfo *hashJoinInfo,
        suez::turing::AttributeExpressionTyped<T> *joinAttrExpr) {
    SingleLayerSeekResult result;

    SingleLayerSearcher singleLayerSearcher(param._queryExecutor,
            param._layerMeta, param._filterWrapper, param._deletionMapReader,
            param._matchDocAllocator, param._timeoutTerminator,
            param._main2SubIt, param._subDeletionMapReader, NULL,
            param._getAllSubDoc, hashJoinInfo, joinAttrExpr);

    auto matchDocAllocator = param._matchDocAllocator;
    auto ec = IE_NAMESPACE(common)::ErrorCode::OK;
    while (true) {
        matchdoc::MatchDoc matchDoc;
        ec = singleLayerSearcher.seekAndJoin<T>(needSubDoc, matchDoc);
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

template <typename T>
SingleLayerSeekResult RankSearcher::doSearchSingleLayerWithScoreAndJoin(
        const SingleLayerSearcherParam &param,
        bool needSubDoc,
        bool needFlatten,
        Aggregator *aggregator,
        HA3_NS(rank)::HitCollectorBase *hitCollector,
        MatchDataManager *manager,
        HA3_NS(common)::HashJoinInfo *hashJoinInfo,
        suez::turing::AttributeExpressionTyped<T> *joinAttrExpr)
{
    SingleLayerSearcher singleLayerSearcher(param._queryExecutor,
            param._layerMeta, param._filterWrapper, param._deletionMapReader,
            param._matchDocAllocator, param._timeoutTerminator,
            param._main2SubIt, param._subDeletionMapReader, manager,
            param._getAllSubDoc, hashJoinInfo, joinAttrExpr);
    auto ec = IE_NAMESPACE(common)::ErrorCode::OK;

    while (true) {
        matchdoc::MatchDoc matchDoc;
        ec = singleLayerSearcher.seekAndJoin<T>(needSubDoc, matchDoc);
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

HA3_TYPEDEF_PTR(RankSearcher);

END_HA3_NAMESPACE(search);
#endif //ISEARCH_RANKSEARCHER_H
