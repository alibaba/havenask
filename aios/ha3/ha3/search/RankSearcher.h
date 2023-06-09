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

#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"
#include "indexlib/partition/index_partition_reader.h"
#include "suez/turing/expression/framework/RankAttributeExpression.h"
#include "suez/turing/expression/function/FunctionManager.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/search/HashJoinInfo.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/config/AggSamplerConfigInfo.h"
#include "ha3/func_expression/FunctionProvider.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/rank/GlobalMatchData.h"
#include "ha3/rank/HitCollectorBase.h"
#include "ha3/search/Aggregator.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/SingleLayerSearcher.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace search {

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
            indexlib::index::DeletionMapReaderAdaptor *deletionMapReader,
            common::Ha3MatchDocAllocator *matchDocAllocator,
            common::TimeoutTerminator *timeoutTerminator,
            indexlib::index::JoinDocidAttributeIterator *main2SubIt,
            indexlib::index::DeletionMapReader *subDeletionMapReader,
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
    indexlib::index::JoinDocidAttributeIterator *_main2SubIt;
    indexlib::index::DeletionMapReaderAdaptor *_deletionMapReader;
    indexlib::index::DeletionMapReader *_subDeletionMapReader;
    bool _getAllSubDoc;
};

struct SingleLayerSeekResult {
    SingleLayerSeekResult()
        : _seekCount(0)
        , _matchCount(0)
        , _leftQuota(0)
        , _seekDocCount(0)
        , _errorCode(indexlib::index::ErrorCode::OK)
    {}
public:
    uint32_t _seekCount;
    uint32_t _matchCount;
    uint32_t _leftQuota;
    uint32_t _seekDocCount;
    indexlib::index::ErrorCode _errorCode;
};

class RankSearcher
{
public:
    typedef indexlib::index::JoinDocidAttributeIterator DocMapAttrIterator;

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
                            search::HashJoinInfo *hashJoinInfo,
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
	    search::HashJoinInfo *hashJoinInfo
	)__attribute__((noinline));

    template <typename T>
    SingleLayerSeekResult doSearchSingleLayerWithScoreAndJoin(
            const SingleLayerSearcherParam &param,
            bool needSubDoc,
            bool needFlatten,
            Aggregator *aggregator,
            rank::HitCollectorBase *hitCollector,
            MatchDataManager *manager,
            search::HashJoinInfo *hashJoinInfo,
            suez::turing::AttributeExpressionTyped<T> *joinAttrExpr)
            __attribute__((noinline));

    SingleLayerSeekResult searchSingleLayerJoinWithoutScore(
            const SingleLayerSearcherParam &param,
            bool needSubDoc,
            Aggregator *aggregator,
            search::HashJoinInfo *hashJoinInfo)
            __attribute__((noinline));

    template <typename T>
    SingleLayerSeekResult doSearchSingleLayerJoinWithoutScore(
            const SingleLayerSearcherParam &param,
            bool needSubDoc,
            Aggregator *aggregator,
            search::HashJoinInfo *hashJoinInfo,
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
                              search::HashJoinInfo *hashJoinInfo);

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
    std::shared_ptr<indexlib::index::InvertedIndexReader> _primaryKeyReader;
    std::vector<QueryExecutor *> _queryExecutors;
    FilterWrapper *_filterWrapper;
    Aggregator *_aggregator;
    common::TimeoutTerminator *_timeoutTerminator;
    common::MultiErrorResultPtr _errorResultPtr;
    LayerMetas *_layerMetas;
    MatchDataManager *_matchDataManager;
    uint32_t _matchCount;
    common::Tracer *_tracer;
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> _delMapReader;
    indexlib::index::DeletionMapReaderPtr _subDelMapReader;
    DocMapAttrIterator *_mainToSubIt;
private:
    friend class MatchDocSearcherTest;
private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
SingleLayerSeekResult RankSearcher::doSearchSingleLayerJoinWithoutScore(
        const SingleLayerSearcherParam &param,
        bool needSubDoc,
        Aggregator *aggregator,
        search::HashJoinInfo *hashJoinInfo,
        suez::turing::AttributeExpressionTyped<T> *joinAttrExpr) {
    SingleLayerSeekResult result;

    SingleLayerSearcher singleLayerSearcher(param._queryExecutor,
            param._layerMeta, param._filterWrapper, param._deletionMapReader,
            param._matchDocAllocator, param._timeoutTerminator,
            param._main2SubIt, param._subDeletionMapReader, NULL,
            param._getAllSubDoc, hashJoinInfo, joinAttrExpr);

    auto matchDocAllocator = param._matchDocAllocator;
    auto ec = indexlib::index::ErrorCode::OK;
    while (true) {
        matchdoc::MatchDoc matchDoc;
        ec = singleLayerSearcher.seekAndJoin<T>(needSubDoc, matchDoc);
        if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
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
        isearch::rank::HitCollectorBase *hitCollector,
        MatchDataManager *manager,
        isearch::search::HashJoinInfo *hashJoinInfo,
        suez::turing::AttributeExpressionTyped<T> *joinAttrExpr)
{
    SingleLayerSearcher singleLayerSearcher(param._queryExecutor,
            param._layerMeta, param._filterWrapper, param._deletionMapReader,
            param._matchDocAllocator, param._timeoutTerminator,
            param._main2SubIt, param._subDeletionMapReader, manager,
            param._getAllSubDoc, hashJoinInfo, joinAttrExpr);
    auto ec = indexlib::index::ErrorCode::OK;

    while (true) {
        matchdoc::MatchDoc matchDoc;
        ec = singleLayerSearcher.seekAndJoin<T>(needSubDoc, matchDoc);
        if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
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

typedef std::shared_ptr<RankSearcher> RankSearcherPtr;

} // namespace search
} // namespace isearch
