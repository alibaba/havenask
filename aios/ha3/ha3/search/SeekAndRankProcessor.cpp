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
#include "ha3/search/SeekAndRankProcessor.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#ifndef AIOS_OPEN_SOURCE
#include "kvtracer/KVTracer.h"
#endif
#include "suez/turing/common/KvTracerMacro.h"
#include "suez/turing/expression/framework/RankAttributeExpression.h"

#include "ha3/common/DistinctClause.h"
#include "ha3/common/DistinctDescription.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/rank/HitCollectorBase.h"
#include "ha3/rank/RankProfile.h"
#include "ha3/rank/ScoringProvider.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/HitCollectorManager.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearchStrategy.h"
#include "ha3/search/MatchDocSearcherProcessorResource.h"
#include "ha3/search/RankSearcher.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/search/SortExpressionCreator.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
class HashJoinInfo;
}  // namespace common
namespace config {
class AggSamplerConfigInfo;
}  // namespace config
}  // namespace isearch

using namespace std;
using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SeekAndRankProcessor);

SeekAndRankProcessor::SeekAndRankProcessor(
        SearchCommonResource &resource,
        SearchPartitionResource &partitionResouce,
        SearchProcessorResource &processorResource,
        const config::AggSamplerConfigInfo &aggSamplerConfigInfo)
    : _resource(resource)
    , _partitionResource(partitionResouce)
    , _processorResource(processorResource)
    , _rankSearcher(partitionResouce.getTotalDocCount(),
                    resource.pool, aggSamplerConfigInfo)
    , _rankExpression(NULL)
    , _hitCollectorManager(NULL)
{ }

SeekAndRankProcessor::~SeekAndRankProcessor() {
    if (_rankExpression) {
        _rankExpression->endRequest();
        auto &warnInfo = _rankExpression->getWarningInfo();
        if (!warnInfo.empty()) {
            _resource.errorResult->addError(ERROR_RANK_WARNING, warnInfo);
        }
    }
    POOL_DELETE_CLASS(_hitCollectorManager);
}

bool SeekAndRankProcessor::init(const common::Request* request) {
    RankSearcherParam rankSearcherParam;
    rankSearcherParam.request = request;
    rankSearcherParam.attrExprCreator = _partitionResource.attributeExpressionCreator.get();
    rankSearcherParam.matchDataManager = &_partitionResource.matchDataManager;
    rankSearcherParam.readerWrapper = _partitionResource.indexPartitionReaderWrapper.get();
    rankSearcherParam.rankSize = _processorResource.docCountLimits.rankSize;
    rankSearcherParam.timeoutTerminator = _resource.timeoutTerminator;
    rankSearcherParam.tracer = _resource.tracer;
    rankSearcherParam.errorResultPtr = _resource.errorResult;
    rankSearcherParam.matchDocAllocator = _resource.matchDocAllocator.get();
    rankSearcherParam.functionProvider = &_partitionResource.functionProvider;
    rankSearcherParam.layerMetas = _processorResource.layerMetas;

    // create query executors first
    if (!_rankSearcher.init(rankSearcherParam)) {
        return false;
    }

    // init sort expressions
    if (!_processorResource.sortExpressionCreator->init(request)) {
        return false;
    }

    _hitCollectorManager = createHitCollectorManager(request);
    if (!_hitCollectorManager
        || !_hitCollectorManager->init(request, _processorResource.docCountLimits.runtimeTopK))
    {
        AUTIL_LOG(WARN, "init HitCollectorManager failed!");
        return false;
    }

    // rankExpression beginRequest have to go after rankSearcher init (to have QueryExecutor to require unpack matchData)
    _rankExpression = _processorResource.sortExpressionCreator->getRankExpression();
    _processorResource.scoreExpression = _rankExpression;
    if (_rankExpression &&
        !_rankExpression->beginRequest(_processorResource.scoringProvider)) {
        std::string errorMsg = "Scorer begin request failed : [" +
                               _processorResource.scoringProvider->getErrorMessage() + "]";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        _resource.errorResult->addError(ERROR_SCORER_BEGIN_REQUEST, errorMsg);
        return false;
    }

    // optimize have to go after rankExpression beginRequest (to set isForceSocre)
    if (_processorResource.rankProfile) {
        optimizeHitCollector(request, _hitCollectorManager,
                             _processorResource.rankProfile->getPhaseCount());
    }
    return true;
}

SeekAndRankResult SeekAndRankProcessor::process(const common::Request *request,
        MatchDocSearchStrategy *searchStrategy,
        InnerSearchResult& innerResult)
{
    KVTRACE_MODULE_SCOPE_WITH_TRACER(
            _resource.sessionMetricsCollector->getTracer(), "rank");
    RankSearcherResource resource;
    resource._needFlattenMatchDoc =
        _resource.matchDocAllocator->hasSubDocAllocator() &&
        request->getConfigClause()->getSubDocDisplayType() == SUB_DOC_DISPLAY_FLAT;
    resource._requiredTopK = _processorResource.docCountLimits.requiredTopK;
    resource._rankSize = _processorResource.docCountLimits.rankSize;
    resource._matchDocAllocator = _resource.matchDocAllocator.get();
    resource._sessionMetricsCollector = _resource.sessionMetricsCollector;
    resource._getAllSubDoc = request->getConfigClause()->getAllSubDoc();

    rank::HitCollectorBase *rankCollector =
        _hitCollectorManager->getRankHitCollector();
    innerResult.totalMatchDocs = _rankSearcher.search(resource, rankCollector);
    innerResult.actualMatchDocs = _rankSearcher.getMatchCount();

    uint32_t deletedDocCount = rankCollector->getDeletedDocCount();
    innerResult.totalMatchDocs -= deletedDocCount;
    innerResult.actualMatchDocs -= deletedDocCount;

    _rankSearcher.collectAggregateResults(innerResult.aggResultsPtr);

    searchStrategy->afterSeek(_hitCollectorManager);
    rankCollector->stealAllMatchDocs(innerResult.matchDocVec);
    searchStrategy->afterStealFromCollector(innerResult, _hitCollectorManager);

    SeekAndRankResult result;
    result.ranked =  _rankExpression && rankCollector->isScored();
    if (rankCollector->getType() != rank::HitCollectorBase::HCT_MULTI)
        result.rankComp = rankCollector->getComparator();
    return result;
}

SeekAndRankResult SeekAndRankProcessor::processWithCache(const common::Request *request,
        SearcherCacheInfo *cacheInfo,
        InnerSearchResult& innerResult)
{
    KVTRACE_MODULE_SCOPE_WITH_TRACER(
            _resource.sessionMetricsCollector->getTracer(), "rank");
    RankSearcherResource resource;
    resource._needFlattenMatchDoc =
        _resource.matchDocAllocator->hasSubDocAllocator() &&
        request->getConfigClause()->getSubDocDisplayType() == SUB_DOC_DISPLAY_FLAT;
    resource._requiredTopK = _processorResource.docCountLimits.requiredTopK;
    resource._rankSize = _processorResource.docCountLimits.rankSize;
    resource._matchDocAllocator = _resource.matchDocAllocator.get();
    resource._sessionMetricsCollector = _resource.sessionMetricsCollector;
    resource._getAllSubDoc = request->getConfigClause()->getAllSubDoc();

    rank::HitCollectorBase *rankCollector =
        _hitCollectorManager->getRankHitCollector();
    innerResult.totalMatchDocs = _rankSearcher.search(resource, rankCollector);
    innerResult.actualMatchDocs = _rankSearcher.getMatchCount();

    uint32_t deletedDocCount = rankCollector->getDeletedDocCount();
    innerResult.totalMatchDocs -= deletedDocCount;
    innerResult.actualMatchDocs -= deletedDocCount;

    _rankSearcher.collectAggregateResults(innerResult.aggResultsPtr);

    if (cacheInfo) {
        cacheInfo->fillAfterSeek(_hitCollectorManager);
    }
    rankCollector->stealAllMatchDocs(innerResult.matchDocVec);
    if (cacheInfo) {
        if (cacheInfo->isHit) {
            cacheInfo->fillAfterStealIfHit(request, innerResult, _hitCollectorManager);
        } else {
            cacheInfo->fillAfterStealIfMiss(request, innerResult);
        }
    }
    SeekAndRankResult result;
    result.ranked =  _rankExpression && rankCollector->isScored();
    if (rankCollector->getType() != rank::HitCollectorBase::HCT_MULTI)
        result.rankComp = rankCollector->getComparator();
    return result;
}

SeekAndRankResult SeekAndRankProcessor::processWithCacheAndJoinInfo(
        const common::Request *request,
        SearcherCacheInfo *cacheInfo,
        HashJoinInfo *hashJoinInfo,
        InnerSearchResult &innerResult)
{
    KVTRACE_MODULE_SCOPE_WITH_TRACER(
            _resource.sessionMetricsCollector->getTracer(), "rank");
    RankSearcherResource resource;
    resource._needFlattenMatchDoc =
        _resource.matchDocAllocator->hasSubDocAllocator() &&
        request->getConfigClause()->getSubDocDisplayType() == SUB_DOC_DISPLAY_FLAT;
    resource._requiredTopK = _processorResource.docCountLimits.requiredTopK;
    resource._rankSize = _processorResource.docCountLimits.rankSize;
    resource._matchDocAllocator = _resource.matchDocAllocator.get();
    resource._sessionMetricsCollector = _resource.sessionMetricsCollector;
    resource._getAllSubDoc = request->getConfigClause()->getAllSubDoc();

    rank::HitCollectorBase *rankCollector =
        _hitCollectorManager->getRankHitCollector();
    innerResult.totalMatchDocs = _rankSearcher.searchWithJoin(resource, hashJoinInfo, rankCollector);
    innerResult.actualMatchDocs = _rankSearcher.getMatchCount();

    uint32_t deletedDocCount = rankCollector->getDeletedDocCount();
    innerResult.totalMatchDocs -= deletedDocCount;
    innerResult.actualMatchDocs -= deletedDocCount;

    _rankSearcher.collectAggregateResults(innerResult.aggResultsPtr);

    if (cacheInfo) {
        cacheInfo->fillAfterSeek(_hitCollectorManager);
    }
    rankCollector->stealAllMatchDocs(innerResult.matchDocVec);
    if (cacheInfo) {
        if (cacheInfo->isHit) {
            cacheInfo->fillAfterStealIfHit(request, innerResult, _hitCollectorManager);
        } else {
            cacheInfo->fillAfterStealIfMiss(request, innerResult);
        }
    }
    SeekAndRankResult result;
    result.ranked =  _rankExpression && rankCollector->isScored();
    if (rankCollector->getType() != rank::HitCollectorBase::HCT_MULTI)
        result.rankComp = rankCollector->getComparator();
    return result;
}

// for now, optimize first round only.
void SeekAndRankProcessor::optimizeHitCollector(const common::Request *request,
                          HitCollectorManager *hitCollectorManager,
                          uint32_t phaseCount) const
{
    common::ConfigClause * configClause = request->getConfigClause();
    if(!configClause->isOptimizeRank()){
        return;
    }
    if (phaseCount == 1) {
        return;
    }
    common::DistinctClause *distClause = request->getDistinctClause();
    if (distClause) {
        common::DistinctDescription *desc = distClause->getDistinctDescription(0);
        if (desc && !desc->getReservedFlag()) {
            return;
        }
    }
    bool isForceScore = _processorResource.scoringProvider->hasDeclareVariable();
    rank::HitCollectorBase *hitCollector = hitCollectorManager->getRankHitCollector();
    if (!isForceScore) {
        uint32_t rerankSize = _processorResource.docCountLimits.rerankSize;
        rerankSize = std::min(rerankSize, MAX_RERANK_SIZE);
        hitCollector->enableLazyScore(rerankSize);
        REQUEST_TRACE_WITH_TRACER(_resource.tracer, TRACE3,
                "use lazy score, rerankSize:[ %u ]", rerankSize);
    }
}

HitCollectorManager *SeekAndRankProcessor::createHitCollectorManager(
        const common::Request *request) const
{
    return POOL_NEW_CLASS(_resource.pool, HitCollectorManager,
                          _partitionResource.attributeExpressionCreator.get(),
                          _processorResource.sortExpressionCreator,
                          _resource.pool,
                          _processorResource.comparatorCreator,
                          _resource.matchDocAllocator,
                          _resource.errorResult);
}

} // namespace search
} // namespace isearch
