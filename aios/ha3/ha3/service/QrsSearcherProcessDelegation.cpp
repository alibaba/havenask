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
#include "ha3/service/QrsSearcherProcessDelegation.h"

#ifdef AIOS_OPEN_SOURCE
#include <aios/network/opentelemetry/core/Span.h>
#else
#include <Span.h>
#endif

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "google/protobuf/arena.h"
#include "google/protobuf/message.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ClusterClause.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/DocIdClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/Hit.h"
#include "ha3/common/Hits.h"
#include "ha3/common/LevelClause.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/PBResultFormatter.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/QueryTermVisitor.h"
#include "ha3/common/Request.h"
#include "ha3/common/RequestSymbolDefine.h"
#include "ha3/common/Result.h"
#include "ha3/common/SummaryHit.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proxy/Merger.h"
#include "ha3/qrs/MatchDocs2Hits.h"
#include "ha3/qrs/QrsProcessor.h"
#include "ha3/search/SearchPluginResource.h"
#include "ha3/service/HitSummarySchemaCache.h"
#include "ha3/service/QrsResponse.h"
#include "ha3/turing/qrs/Ha3QrsRequestGenerator.h"
#include "ha3/turing/qrs/QrsRunGraphContext.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/GrpcResponse.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "aios/network/gig/multi_call/interface/Response.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "aios/network/gig/multi_call/interface/SyncClosure.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/proto/Search.pb.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/protobuf/config.pb.h"

using namespace std;
using namespace autil;
using namespace multi_call;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::proto;
using namespace isearch::util;
using namespace isearch::config;
using namespace isearch::search;
using namespace isearch::common;
using namespace isearch::qrs;
using namespace isearch::proxy;
using namespace isearch::turing;
namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, QrsSearcherProcessDelegation);

QrsSearcherProcessDelegation::QrsSearcherProcessDelegation(
        const HitSummarySchemaCachePtr &hitSummarySchemaCache,
        int32_t connectionTimeout,
        autil::CompressType requestCompressType,
        const config::SearchTaskQueueRule &searchTaskqueueRule)
    : _arena(new google::protobuf::Arena())
    , _hitSummarySchemaCache(hitSummarySchemaCache)
    , _connectionTimeout(connectionTimeout)
    , _requestCompressType(requestCompressType)
    , _searchTaskqueueRule(searchTaskqueueRule)
    , _lackResult(false)
    , _hasReSearch(false)
    , _phase1Version(multi_call::INVALID_VERSION_ID)
    , _phase2Version(multi_call::INVALID_VERSION_ID)
{
}

QrsSearcherProcessDelegation::QrsSearcherProcessDelegation(
        const QrsSearcherProcessDelegation& delegation)
    : _arena(new google::protobuf::Arena())
    , _hitSummarySchemaCache(delegation._hitSummarySchemaCache)
    , _connectionTimeout(delegation._connectionTimeout)
    , _requestCompressType(delegation._requestCompressType)
    , _searchTaskqueueRule(delegation._searchTaskqueueRule)
    , _lackResult(false)
    , _hasReSearch(false)
    , _phase1Version(multi_call::INVALID_VERSION_ID)
    , _phase2Version(multi_call::INVALID_VERSION_ID)
{
}

QrsSearcherProcessDelegation::~QrsSearcherProcessDelegation() {
    clearResource();
}

ResultPtr QrsSearcherProcessDelegation::search(common::RequestPtr &requestPtr) {
    assert(_metricsCollectorPtr != NULL);
    _metricsCollectorPtr->processChainFirstTripTrigger();
    _memPool = requestPtr->getPool();
    createClusterIdMap();
    ConfigClause *configClause = requestPtr->getConfigClause();
    configClause->setPhaseNumber(SEARCH_PHASE_ONE);
    vector<string> clusterNameVec = configClause->getClusterNameVector();
    LevelClause *levelClause = requestPtr->getLevelClause();
    if (levelClause
        && levelClause->useLevel()
        && levelClause->getLevelQueryType() == BOTH_LEVEL)
    {
        const ClusterLists clusterLists = levelClause->getLevelClusters();
        for (ClusterLists::const_iterator it = clusterLists.begin();
             it != clusterLists.end(); it++)
        {
            addClusterName(*it, clusterNameVec);
        }
    }

    MultiErrorResultPtr errors(new MultiErrorResult);
    ResultPtrVector results;
    doSearchAndResearch("search", clusterNameVec, requestPtr, results, errors);

    uint32_t nowDocCount = getMatchDocCount(results);
    if (needSearchLevel(levelClause)
        && nowDocCount < levelClause->getMinDocs())
    {
        _hasReSearch = true;
        uint32_t levelCount = 0;
        _metricsCollectorPtr->searchMultiLevelStartTrigger();
        const ClusterLists &clusterLists = levelClause->getLevelClusters();
        for (ClusterLists::const_iterator it = clusterLists.begin();
             it != clusterLists.end() && nowDocCount < levelClause->getMinDocs();
             it++)
        {
            addClusterName(*it, clusterNameVec);
            stringstream ss;
            ss << "level_" << levelCount << "_search";
            ResultPtrVector nextLevelResults;
            doSearchAndResearch(ss.str(), *it, requestPtr, nextLevelResults, errors);
            results.insert(results.end(), nextLevelResults.begin(), nextLevelResults.end());
            nowDocCount += getMatchDocCount(nextLevelResults);
            levelCount++;
        }
        _metricsCollectorPtr->searchMultiLevelEndTrigger();
        _metricsCollectorPtr->searchMultiLevelCountTrigger(levelCount);
    }

    _metricsCollectorPtr->setReceivedMatchDocCount(getMatchDocCount(results));
    ResultPtr mergedResultPtr(new common::Result);
    merge(requestPtr, results, mergedResultPtr);
    if (errors->hasError()) {
        mergedResultPtr->addErrorResult(errors);
    }
    _metricsCollectorPtr->respondentChildCountPhase1Trigger(results.size());
    checkCoveredRanges(clusterNameVec, mergedResultPtr->getCoveredRanges());
    if (configClause->hasPBMatchDocFormat()) {
        if (associateClusterName(mergedResultPtr)) {
            selectMatchDocsToFormat(requestPtr, mergedResultPtr);
        }
        updateTotalMatchDocs(requestPtr, mergedResultPtr);
    } else {
        convertMatchDocsToHits(requestPtr, mergedResultPtr, _clusterNames);
    }
    convertAggregateResults(requestPtr, mergedResultPtr);
    _metricsCollectorPtr->processChainLastTripTrigger();
    mergedResultPtr->setLackResult(_lackResult);
    return mergedResultPtr;
}

void QrsSearcherProcessDelegation::doSearchAndResearch(
        const std::string &methodName,
        const vector<string> &clusterNameVec,
        const common::RequestPtr &requestPtr,
        ResultPtrVector &results, MultiErrorResultPtr &errors)
{
    if (_querySession) {
        _querySession->putUserData(HA_RESERVED_METHOD, methodName);
    }
    doSearch(clusterNameVec, requestPtr, results, errors);

    if (!errors->hasError() && needResearch(requestPtr, results)) {
        _metricsCollectorPtr->increaseResearchQps();
        ConfigClause *configClause = requestPtr->getConfigClause();
        configClause->setUseTruncateOptimizer(false);
        results.clear();
        if (_querySession) {
            _querySession->putUserData(HA_RESERVED_METHOD, methodName + "_research");
        }
        doSearch(clusterNameVec, requestPtr, results, errors);
    }
}

void QrsSearcherProcessDelegation::doSearch(
        const vector<string> &clusterNameVec,
        const common::RequestPtr &requestPtr,
        ResultPtrVector &resultPtrVec, MultiErrorResultPtr &errors)
{
    _metricsCollectorPtr->sendPhase1StartTrigger();
    ReplyPtr reply = doAsyncProcessCallPhase1(
            clusterNameVec, requestPtr, errors);
    _metricsCollectorPtr->sendPhase1EndTrigger();

    if (!reply) {
        AUTIL_LOG(ERROR, "ReplyPtr is NULL");
        return;
    }

    _metricsCollectorPtr->waitPhase1StartTrigger();
    if (!waitResults()) {
        AUTIL_LOG(WARN, "qrs wait phase 1 Results failed");
        _metricsCollectorPtr->waitPhase1EndTrigger();
        _phase1Version = multi_call::INVALID_VERSION_ID;
    } else {
        _metricsCollectorPtr->waitPhase1EndTrigger();
        _phase1Version = fillResults(reply, resultPtrVec, errors);
        _metricsCollectorPtr->fillPhase1ResultTrigger();
    }
}

bool QrsSearcherProcessDelegation::needResearch(
        const common::RequestPtr &requestPtr,
        const ResultPtrVector &results)
{
    if (requestPtr->getOptimizerClause() == NULL) {
        return false;
    }
    ConfigClause *configClause = requestPtr->getConfigClause();
    uint32_t researchThreshold = configClause->getResearchThreshold();
    if (researchThreshold == 0) {
        return false;
    }
    bool triggerTruncateOptimizer = false;
    uint32_t totalMatchCount = 0;
    for (ResultPtrVector::const_iterator it = results.begin();
         it != results.end(); it++)
    {
        const ResultPtr &result = *it;
        triggerTruncateOptimizer = triggerTruncateOptimizer
                                   || result->useTruncateOptimizer();
        totalMatchCount += result->getActualMatchDocs();
    }
    if (!triggerTruncateOptimizer) {
        return false;
    }
    return totalMatchCount < researchThreshold;
}

bool  QrsSearcherProcessDelegation::needSearchLevel(const LevelClause *levelClause)
{
    if (!levelClause) {
        return false;
    }
    if (!levelClause->useLevel()) {
        return false;
    }
    if (levelClause->getLevelQueryType() != CHECK_FIRST_LEVEL) {
        return false;
    }
    return true;
}

int QrsSearcherProcessDelegation::getMatchDocCount(const ResultPtrVector &results)
{
    uint32_t docCount = 0;
    for (ResultPtrVector::const_iterator it = results.begin();
         it != results.end(); ++it)
    {
        docCount += (*it)->getActualMatchDocs();
    }
    return docCount;
}

ResultPtr QrsSearcherProcessDelegation::runGraph(const common::RequestPtr& request,
        const ResultPtrVector &results)
{
    ResultPtr result;
    std::vector<tensorflow::Tensor> outputs;
    auto closure = [&outputs, &result](const Status &s) {
        if (s.ok() && outputs.size() == 1) {
            auto ha3ResultVariant = outputs[0].scalar<Variant>()().get<Ha3ResultVariant>();
            if (ha3ResultVariant) {
                result = ha3ResultVariant->getResult();
                return;
            }
        }
        result.reset(new common::Result());
        ErrorResult error(ERROR_RUN_QRS_GRAPH_FAILED, s.ToString());
        result->addErrorResult(error);
    };
    tensorflow::RunMetadata runMetadata;
    _runGraphContext->setRequest(request);
    _runGraphContext->setResults(results);
    _runGraphContext->run(&outputs, &runMetadata, std::move(closure));
    return result;
}

void QrsSearcherProcessDelegation::merge(const common::RequestPtr &requestPtr,
        const ResultPtrVector &results,
        ResultPtr &mergedResultPtr)
{
    _metricsCollectorPtr->mergePhase1StartTrigger();
    int64_t begTime = TimeUtility::currentTime();
    mergedResultPtr = runGraph(requestPtr, results);
    int64_t endTime = TimeUtility::currentTime();
    _metricsCollectorPtr->setQrsRunGraphTime((endTime-begTime)/1000.0);
    _metricsCollectorPtr->mergePhase1EndTrigger();
}

void QrsSearcherProcessDelegation::addClusterName(const string &clusterName,
        vector<string> &clusterNameVec)
{
    if (find(clusterNameVec.begin(), clusterNameVec.end(),
             clusterName) == clusterNameVec.end())
    {
        clusterNameVec.push_back(clusterName);
    }
}

void QrsSearcherProcessDelegation::addClusterName(const vector<string> &fromClusterNameVec,
        vector<string> &clusterNameVec)
{
    for (vector<string>::const_iterator it = fromClusterNameVec.begin();
         it != fromClusterNameVec.end(); it++)
    {
        addClusterName(*it, clusterNameVec);
    }
}

string QrsSearcherProcessDelegation::genHitSummarySchemaCacheKey(
        const string &clusterName,
        ConfigClause *configClause)
{
    const string &hitSummarySchemaCacheKey = configClause->getHitSummarySchemaCacheKey();
    if (hitSummarySchemaCacheKey.empty()) {
        return clusterName;
    }
    return clusterName + CLAUSE_MULTIVALUE_SEPERATOR + hitSummarySchemaCacheKey;
}

void QrsSearcherProcessDelegation::fillSummary(
        const common::RequestPtr &requestPtr,
        const ResultPtr &resultPtr)
{
    _memPool = requestPtr->getPool();
    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = createDocIdClauseMap(requestPtr,
            resultPtr, hitSummarySchemas);
    assert(_metricsCollectorPtr != NULL);
    _metricsCollectorPtr->processChainFirstTripTrigger();
    if (!docIdClauseMap.empty()) {
        MultiErrorResultPtr errors(new MultiErrorResult);
        ResultPtrVector resultPtrVec;
        if (_querySession) {
            _querySession->putUserData(HA_RESERVED_METHOD, "fetch_summary");
        }
        doFillSummary(docIdClauseMap, requestPtr, resultPtrVec, errors);
        _metricsCollectorPtr->mergePhase2StartTrigger();
        // fill multi_call errors first
        if (resultPtr && errors->hasError()) {
            resultPtr->addErrorResult(errors);
        }
        Merger::mergeErrors(resultPtr.get(), resultPtrVec);
        Merger::mergeTracer(requestPtr.get(), resultPtr.get(), resultPtrVec);
        uint32_t summaryLackCount =
            Merger::mergeSummary(resultPtr.get(), resultPtrVec,
                    requestPtr->getConfigClause()->allowLackOfSummary());
        Merger::mergeTracer(_metricsCollectorPtr->getTracer(), resultPtr);
        _metricsCollectorPtr->mergePhase2EndTrigger();
        _metricsCollectorPtr->setSummaryLackCount(summaryLackCount);
        if (!(requestPtr->getConfigClause()->allowLackOfSummary())) {
            _metricsCollectorPtr->setUnexpectedSummaryLackCount(summaryLackCount);
        }
        _metricsCollectorPtr->respondentChildCountPhase2Trigger(
                resultPtrVec.size());
        if (resultPtr) {
            updateHitSummarySchemaAndFillCache(resultPtr->getHits(),
                    hitSummarySchemas,
                    requestPtr);
            resultPtr->setLackResult(_lackResult);
        }
    } else {
        AUTIL_LOG(DEBUG, "docIdClauseMap is empty");
    }
    _metricsCollectorPtr->processChainLastTripTrigger();
}

void QrsSearcherProcessDelegation::updateHitSummarySchemaAndFillCache(Hits* hits,
        const vector<HitSummarySchemaPtr> &hitSummarySchemas,
        const common::RequestPtr &requestPtr)
{
    if (!hits) {
        return;
    }

    HitSummarySchemaMap cachedSummarySchemaMap;

    for (vector<HitSummarySchemaPtr>::const_iterator it = hitSummarySchemas.begin();
         it != hitSummarySchemas.end(); ++it)
    {
        cachedSummarySchemaMap[(*it)->getSignatureNoCalc()] = (*it).get();
    }

    HitSummarySchemaMap &hitSummarySchemaMap = hits->getHitSummarySchemaMap();

    map<string, HitSummarySchema*> updateCacheMap;
    for (uint32_t i = 0; i < hits->size(); ++i) {
        const HitPtr &hitPtr = hits->getHit(i);
        SummaryHit *summaryHit = hitPtr->getSummaryHit();
        if (!summaryHit) {
            continue;
        }
        int64_t signature = summaryHit->getSignature();
        HitSummarySchemaMap::const_iterator it = hitSummarySchemaMap.find(signature);
        HitSummarySchema *hitSummarySchema = NULL;
        if (it != hitSummarySchemaMap.end()) {
            hitSummarySchema = it->second;
            string cacheKey = genHitSummarySchemaCacheKey(hitPtr->getClusterName(),
                    requestPtr->getConfigClause());
            updateCacheMap[cacheKey] = it->second;
            // update hitSummarySchema in hit.
            summaryHit->setHitSummarySchema(hitSummarySchema);
        } else {
            it = cachedSummarySchemaMap.find(signature);
            assert(it != cachedSummarySchemaMap.end());
            // do not use the pointer in cache, clone one for hits.
            hitSummarySchema = it->second->clone();
            summaryHit->setHitSummarySchema(hitSummarySchema);
            hitSummarySchemaMap[signature] = hitSummarySchema;
        }

    }

    // try to fill hitSummarySchema into cache, replace it if signature is not equal.
    for (map<string, HitSummarySchema*>::const_iterator it = updateCacheMap.begin();
         it != updateCacheMap.end(); ++it)
    {
        _hitSummarySchemaCache->setHitSummarySchema(it->first, it->second);
    }
}

DocIdClauseMap QrsSearcherProcessDelegation::createDocIdClauseMap(
        const common::RequestPtr &requestPtr,
        const ResultPtr &resultPtr,
        vector<HitSummarySchemaPtr> &hitSummarySchemas)
{
    DocIdClauseMap docIdClauseMap;
    if (!requestPtr || !resultPtr) {
        return docIdClauseMap;
    }

    Hits *hits = resultPtr->getHits();
    if (!hits || 0 == hits->size() || hits->hasSummary()) {
        return docIdClauseMap;
    }

    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);

    // extract term vector
    QueryTermVisitor termVisitor;
    assert(requestPtr->getQueryClause());
    Query *query = requestPtr->getQueryClause()->getRootQuery();
    assert(query);
    query->accept(&termVisitor);
    string originString = requestPtr->getQueryClause()->getOriginalString();

    uint32_t size = hits->size();
    DocIdClause *docIdClause = NULL;
    for (uint32_t i = 0; i < size; i ++) {
        const HitPtr &hitPtr = hits->getHit(i);
        const string &phrase1ClusterName = hitPtr->getClusterName();
        const string &phrase2ClusterName =
            configClause->getFetchSummaryClusterName(phrase1ClusterName);
        DocIdClauseMap::iterator it = docIdClauseMap.find(phrase2ClusterName);
        if (it == docIdClauseMap.end()) {
            docIdClause = new DocIdClause();
            docIdClause->setSummaryProfileName(configClause->getSummaryProfileName());
            docIdClause->setQueryString(originString);
            docIdClause->setTermVector(termVisitor.getTermVector());
            string cacheKey = genHitSummarySchemaCacheKey(phrase2ClusterName, configClause);
            HitSummarySchemaPtr schema =
                _hitSummarySchemaCache->getHitSummarySchema(cacheKey);
            if (schema.get() != NULL) {
                docIdClause->setSignature(schema->getSignature());
                hitSummarySchemas.push_back(schema);
                REQUEST_TRACE(DEBUG, "summary schema cache hited, cachekey: [%s]"
                        " schema signature is:%lu", cacheKey.c_str(), schema->getSignature() );

            }
            docIdClauseMap.insert(make_pair(phrase2ClusterName, docIdClause));
        } else {
            docIdClause = it->second;
        }
        GlobalIdentifier globalId = hitPtr->getGlobalIdentifier();
        docIdClause->addGlobalId(globalId);
    }

    return docIdClauseMap;
}

void QrsSearcherProcessDelegation::doFillSummary(
        DocIdClauseMap &docIdClauseMap,
        const common::RequestPtr &requestPtr,
        ResultPtrVector &resultPtrVec,
        MultiErrorResultPtr &errors)
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    configClause->setPhaseNumber(SEARCH_PHASE_TWO);
    _metricsCollectorPtr->sendPhase2StartTrigger();
    ReplyPtr reply = doAsyncProcessCallPhase2(
            docIdClauseMap, requestPtr, errors);
    _metricsCollectorPtr->sendPhase2EndTrigger();
    if (!reply) {
        AUTIL_LOG(WARN, "reply is NULL!");
        return;
    }
    _metricsCollectorPtr->waitPhase2StartTrigger();
    if (!waitResults()) {
        AUTIL_LOG(WARN, "qrs wait phase 2 Results failed");
        _metricsCollectorPtr->waitPhase2EndTrigger();
        _phase2Version = multi_call::INVALID_VERSION_ID;
    } else {
        _metricsCollectorPtr->waitPhase2EndTrigger();
        _phase2Version = fillResults(reply, resultPtrVec, errors);
        _metricsCollectorPtr->fillPhase2ResultTrigger();
    }
}

void QrsSearcherProcessDelegation::convertMatchDocsToHits(
        const common::RequestPtr &requestPtr,
        const ResultPtr &resultPtr,
        const vector<string> &clusterNameVec)
{
    if (!requestPtr || !resultPtr) {
        return;
    }

    MatchDocs2Hits converter;
    ErrorResult errResult;
    Hits *hits = converter.convert(resultPtr->getMatchDocs(),
                                   requestPtr, resultPtr->getSortExprMeta(),
                                   errResult, clusterNameVec);
    if (errResult.hasError()) {
        resultPtr->addErrorResult(errResult);
        resultPtr->clearMatchDocs();
        return ;
    }

    uint32_t actualMatchDocs = resultPtr->getActualMatchDocs();
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (actualMatchDocs < configClause->getActualHitsLimit()) {
        AUTIL_LOG(DEBUG, "actual_hits_limit: %u, actual: %u, total: %u",
                configClause->getActualHitsLimit(), actualMatchDocs,
                resultPtr->getTotalMatchDocs());
        hits->setTotalHits(actualMatchDocs);
    } else {
        hits->setTotalHits(resultPtr->getTotalMatchDocs());
    }
    resultPtr->setHits(hits);
    resultPtr->clearMatchDocs();
}

void QrsSearcherProcessDelegation::convertAggregateResults(
        const common::RequestPtr &requestPtr,
        const ResultPtr &resultPtr)
{
    if (!requestPtr || !resultPtr) {
        return;
    }
    uint32_t aggResultCount = resultPtr->getAggregateResultCount();
    for (uint32_t i = 0; i < aggResultCount; ++i) {
        AggregateResultPtr aggResult = resultPtr->getAggregateResult(i);
        if (aggResult) {
            aggResult->constructGroupValueMap();
        }
    }
}

void QrsSearcherProcessDelegation::setClusterSorterManagers(
        const ClusterSorterManagersPtr &clusterSorterManagerPtr)
{
    _clusterSorterManagersPtr = clusterSorterManagerPtr;
}

int32_t QrsSearcherProcessDelegation::getAndAdjustTimeOut(
        const common::RequestPtr &requestPtr)
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (configClause->getRpcTimeOut() == 0) {
        configClause->setRpcTimeOut(_connectionTimeout);
    }

    int64_t rpcTimeout =  configClause->getRpcTimeOut();
    int64_t leftTime = numeric_limits<int64_t>::max();
    if (_timeoutTerminator) {
        leftTime = _timeoutTerminator->getLeftTime() / 1000;  // ms
    }
    rpcTimeout = min(rpcTimeout, leftTime);
    if(rpcTimeout <= 0) {
        rpcTimeout = _connectionTimeout;
        AUTIL_LOG(WARN, "rpcTimeout [%ld] invaid, set connectionTimeout [%d]",
                rpcTimeout, _connectionTimeout);
    }
    configClause->setRpcTimeOut(rpcTimeout);
    AUTIL_LOG(DEBUG, "adjust rpc timeOut:%ld", rpcTimeout);
    return rpcTimeout;
}

bool QrsSearcherProcessDelegation::waitResults() {

    int ret = 0;
    if (_qrsClosure) {
        ret = _qrsClosure->wait();
    }
    return ret == 0;
}

multi_call::VersionTy QrsSearcherProcessDelegation::fillResults(
        const ReplyPtr &reply,
        ResultPtrVector &resultVec,
        MultiErrorResultPtr &errors)
{
    size_t lackCount = 0;
    const auto &allResponses = reply->getResponses(lackCount);
    _lackResult = lackCount != 0;
    if (allResponses.empty()) {
        errors->addError(ERROR_QRS_MULTI_CALL_ERROR,
                         "no provider or full degrade triggered");
    }
    multi_call::VersionTy version = multi_call::INVALID_VERSION_ID;
    for (const auto &response : allResponses) {
        ResultPtr resultPtr;
        if (!response->isFailed()) {
            if (!fillResult(response, resultPtr)) {
                AUTIL_LOG(WARN, "Response is invalid");
                resultPtr.reset(new common::Result(ERROR_SEARCH_RESPONSE));
            }
            version = response->getVersion();
            const string &clusterName = response->getBizName();
            resultPtr->setClusterInfo(clusterName, getClusterId(clusterName));
        } else {
            auto ec = response->getErrorCode();
            string errMsg = reply->getErrorMessage(ec);
            resultPtr.reset(new common::Result(ERROR_SEARCH_RESPONSE, errMsg));
            errors->addError(ERROR_QRS_MULTI_CALL_ERROR, errMsg);
        }
        resultPtr->setErrorHostInfo(response->getNodeId(), response->getSpecStr());
        resultVec.push_back(resultPtr);
    }
    return version;
}

bool QrsSearcherProcessDelegation::fillResult(const ResponsePtr& response, ResultPtr &resultPtr) {
    GraphResponse *message = NULL;
    auto qrsResponse = dynamic_cast<service::QrsResponse *>(response.get());
    auto gigQrsResponse = dynamic_cast<multi_call::GrpcResponse *>(response.get());
    if (qrsResponse) {
        message = dynamic_cast<GraphResponse *>(qrsResponse->getMessage());
    } else if (gigQrsResponse) {
        message = (GraphResponse *)gigQrsResponse->getMessage(
            google::protobuf::Arena::CreateMessage<GraphResponse>(
                _arena.get()));
    }
    if (!message) {
        return false;
    }
    if (message->outputs_size() != 1) {
        return false;
    }
    const tensorflow::TensorProto &tensorProto =  message->outputs(0).tensor();
    if (tensorProto.dtype() != DT_VARIANT) {
        return false;
    }
    auto responseTensor = Tensor(DT_VARIANT, TensorShape({}));
    if (!responseTensor.FromProto(tensorProto)) {
        return false;
    }
    auto ha3ResultVariant = responseTensor.scalar<Variant>()().get<Ha3ResultVariant>();
    if (!ha3ResultVariant) {
        return false;
    }

    ha3ResultVariant->construct(_memPool);
    resultPtr = ha3ResultVariant->getResult();
    return true;
}

void QrsSearcherProcessDelegation::checkCoveredRanges(
    const vector<string> &clusterNameVec, common::Result::ClusterPartitionRanges &coveredRanges) {
    if (coveredRanges.size() != clusterNameVec.size()) {
        for (vector<string>::const_iterator it = clusterNameVec.begin();
             it != clusterNameVec.end(); ++it)
        {
            const string& clusterName = *it;
            if (coveredRanges.find(clusterName) == coveredRanges.end()) {
                coveredRanges[clusterName] = common::Result::PartitionRanges();
            }
        }
    }
}

void QrsSearcherProcessDelegation::selectMatchDocsToFormat(
        const common::RequestPtr &requestPtr,
        ResultPtr &resultPtr)
{
    if (!requestPtr || !resultPtr) {
        return;
    }
    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);
    uint32_t startOffset = configClause->getStartOffset();
    uint32_t hitCount = configClause->getHitCount();
    auto &matchDocVec = resultPtr->getMatchDocsForFormat();
    matchDocVec.clear();
    MatchDocs *matchDocs = resultPtr->getMatchDocs();
    if (!matchDocs) {
        return;
    }
    uint32_t matchDocsSize = matchDocs->size();
    if (matchDocsSize > hitCount + startOffset) {
        matchDocsSize = hitCount + startOffset;
    }
    if (startOffset < matchDocsSize) {
        matchDocVec.reserve(matchDocsSize - startOffset);
    }
    for (uint32_t i = startOffset; i < matchDocsSize; ++i) {
        matchDocVec.push_back(matchDocs->getMatchDoc(i));
    }
}

ReplyPtr QrsSearcherProcessDelegation::doAsyncProcessCallPhase1(
        const vector<string> &clusterNameVec,
        const common::RequestPtr &requestPtr,
        MultiErrorResultPtr &errors)
{
    clearResource();
    _qrsClosure = new multi_call::SyncClosure();
    auto clusterClause = requestPtr->getClusterClause();
    turing::GenerateType generateType = turing::GT_NORMAL;
    if (clusterClause && !clusterClause->emptyPartIds()) {
        generateType = turing::GT_PID;
    }
    auto version = requestPtr->getConfigClause()->getVersion();
    auto sourceId = getSourceId(requestPtr);
    std::string benchMarkKey;
    if (_querySession) {
        benchMarkKey = _querySession->getUserData(HA_RESERVED_BENCHMARK_KEY);
    }
    suez::RpcServer *rpcServer = nullptr;
    if (_runGraphContext) {
        rpcServer = _runGraphContext->getRpcServer();
    }
    for (size_t i = 0; i < clusterNameVec.size(); ++i) {
        const auto &bizName = clusterNameVec[i];
        auto generator = turing::Ha3QrsRequestGeneratorPtr(
                new turing::Ha3QrsRequestGenerator(
                        bizName, sourceId, version,
                        generateType, requestPtr->useGrpc(), requestPtr,
                        getAndAdjustTimeOut(requestPtr),
                        NULL, _searchTaskqueueRule, _memPool,
                        _requestCompressType, _arena, rpcServer));
        generator->setPreferVersion(_phase1Version);
        if (HA_RESERVED_BENCHMARK_VALUE_1 == benchMarkKey
            || HA_RESERVED_BENCHMARK_VALUE_2 == benchMarkKey)
        {
            generator->setFlowControlStrategy(getBenchmarkBizName(bizName));
        }
        _generatorVec.push_back(generator);
    }
    return multiCall(requestPtr, errors);
}

ReplyPtr QrsSearcherProcessDelegation::multiCall(
        const common::RequestPtr &requestPtr,
        MultiErrorResultPtr &errors)
{
    if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
        if (_timeoutTerminator->isTimeout()) {
            string errMsg = "Qrs process timeout.";
            AUTIL_LOG(WARN, "%s, begin [%ld] expire [%ld]", errMsg.c_str(),
                    _timeoutTerminator->getStartTime(), _timeoutTerminator->getExpireTime());
            errors->addError(ERROR_QRS_PROCESS_TIMEOUT, errMsg);
            return ReplyPtr();
        }
    }
    SearchParam param;
    param.closure = _qrsClosure;
    for (const auto &generator : _generatorVec) {
        param.generatorVec.push_back(generator);
    }

    ReplyPtr reply;
    if (_querySession) {
        _querySession->call(param, reply);
    }
    return reply;
}

void QrsSearcherProcessDelegation::clearResource() {
    DELETE_AND_SET_NULL(_qrsClosure);
    _generatorVec.clear();
    _lackResult = false;
}

ReplyPtr QrsSearcherProcessDelegation::doAsyncProcessCallPhase2(
        const DocIdClauseMap &docIdClauseMap,
        const common::RequestPtr &requestPtr,
        MultiErrorResultPtr &errors)
{
    clearResource();
    _qrsClosure = new multi_call::SyncClosure();
    auto version = requestPtr->getConfigClause()->getVersion();
    auto sourceId = getSourceId(requestPtr);
    std::string benchMarkKey;
    if (_querySession) {
        benchMarkKey = _querySession->getUserData(HA_RESERVED_BENCHMARK_KEY);
    }
    suez::RpcServer *rpcServer = nullptr;
    if (_runGraphContext) {
        rpcServer = _runGraphContext->getRpcServer();
    }
    for (DocIdClauseMap::const_iterator it = docIdClauseMap.begin();
         it != docIdClauseMap.end(); ++it)
    {
        const auto &bizName = it->first;
        auto generator = turing::Ha3QrsRequestGeneratorPtr(
                new Ha3QrsRequestGenerator(it->first, sourceId, version,
                        turing::GT_SUMMARY, requestPtr->useGrpc(), requestPtr, getAndAdjustTimeOut(requestPtr),
                        it->second, _searchTaskqueueRule,
                        _memPool, _requestCompressType, _arena, rpcServer));
        generator->setPreferVersion(_phase2Version);
        if (HA_RESERVED_BENCHMARK_VALUE_1 == benchMarkKey
            || HA_RESERVED_BENCHMARK_VALUE_2 == benchMarkKey)
        {
            generator->setFlowControlStrategy(getBenchmarkBizName(bizName));
        }
        _generatorVec.push_back(generator);
    }
    return multiCall(requestPtr, errors);
}

void QrsSearcherProcessDelegation::updateTotalMatchDocs(
        const common::RequestPtr &requestPtr,
        const ResultPtr &resultPtr)
{
    uint32_t actualMatchDocs = resultPtr->getActualMatchDocs();
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (actualMatchDocs < configClause->getActualHitsLimit()) {
        AUTIL_LOG(DEBUG, "actual_hits_limit: %u, actual: %u, total: %u",
                configClause->getActualHitsLimit(), actualMatchDocs,
                resultPtr->getTotalMatchDocs());
        resultPtr->setTotalMatchDocs(actualMatchDocs);
    }
}

void QrsSearcherProcessDelegation::createClusterIdMap() {
    if (_querySession) {
        _clusterNames = _querySession->getSearchService()->getBizNames();
    }
    sort(_clusterNames.begin(), _clusterNames.end(), std::less<string>());
    for (auto it = _clusterNames.begin(); it != _clusterNames.end(); ++it) {
        const string &clusterName = *it;
        if (_clusterIdMap.find(clusterName) != _clusterIdMap.end()) {
            continue;
        }
        clusterid_t id = _clusterIdMap.size();
        _clusterIdMap[clusterName] = id;
    }
}

clusterid_t QrsSearcherProcessDelegation::getClusterId(const string &clusterName) const
{
    ClusterIdMap::const_iterator it = _clusterIdMap.find(clusterName);
    if (it != _clusterIdMap.end()) {
        return it->second;
    }
    return INVALID_CLUSTERID;
}

SourceIdTy QrsSearcherProcessDelegation::getSourceId(
        const common::RequestPtr &request)
{
    auto sourceId = multi_call::INVALID_SOURCE_ID;
    const auto &sourceIdStr = request->getConfigClause()->getSourceId();
    if (!sourceIdStr.empty()) {
        sourceId = HashAlgorithm::hashString64(sourceIdStr.c_str());
    }
    request->setRowKey(sourceId);
    return sourceId;
}

bool QrsSearcherProcessDelegation::associateClusterName(const ResultPtr &resultPtr) const {
    if (!resultPtr) {
        return false;
    }

    size_t clusterNameSize = _clusterNames.size();
    for (clusterid_t i = 0; i < (clusterid_t)clusterNameSize; i++) {
        resultPtr->addClusterName(_clusterNames[i]);
    }
    return true;
}

void QrsSearcherProcessDelegation::setRunGraphContext(const turing::QrsRunGraphContextPtr &context) {
    _runGraphContext = context;
    if (_runGraphContext) {
        auto *queryResource = _runGraphContext->getQueryResource();
        if (queryResource) {
            _querySession = queryResource->getGigQuerySession();
        }
    }
}

} // namespace service
} // namespace isearch
