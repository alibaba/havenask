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
#include "ha3/proxy/Merger.h"

#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <algorithm>
#include <iosfwd>
#include <map>
#include <memory>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionFactory.h"
#include "suez/turing/expression/framework/SimpleAttributeExpressionCreator.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/plugin/SorterWrapper.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "ha3/common/AggregateClause.h"
#include "ha3/common/AggregateDescription.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/DataProvider.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Hits.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/searchinfo/PhaseOneSearchInfo.h"
#include "ha3/common/searchinfo/PhaseTwoSearchInfo.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/isearch.h"
#include "ha3/proxy/AggFunResultMerger.h"
#include "ha3/proxy/AggResultSort.h"
#include "ha3/proxy/MatchDocDeduper.h"
#include "ha3/proxy/VariableSlabComparator.h"
#include "ha3/rank/ComparatorCreator.h"
#include "ha3/search/SearchPluginResource.h"
#include "ha3/search/SortExpressionCreator.h"
#include "ha3/sorter/SorterProvider.h"
#include "ha3/sorter/SorterResource.h"
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace suez {
namespace turing {
class AttributeExpressionCreatorBase;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace autil::mem_pool;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::rank;
using namespace isearch::util;
using namespace isearch::search;
using namespace isearch::sorter;

namespace isearch {
namespace proxy {
AUTIL_LOG_SETUP(ha3, Merger);

Merger::Merger() {
}

Merger::~Merger() {
}

void Merger::mergeResult(ResultPtr &outputResult,
                         const vector<ResultPtr> &inputResults,
                         const Request *request,
                         SorterWrapper *sorterWrapper,
                         SorterLocation sl)
{
    mergeResults(outputResult.get(), inputResults, request, sorterWrapper, sl);
}

void Merger::mergeResults(common::Result *outputResult,
                          const vector<ResultPtr> &inputResults,
                          const Request *request,
                          SorterWrapper *sorterWrapper,
                          SorterLocation sl) {
    mergeErrors(outputResult, inputResults);
    mergeTracer(request, outputResult, inputResults);

    ConfigClause *configClause = request->getConfigClause();
    assert(configClause);
    // can not dedup in sub_doc_flat mode,
    bool doDedup = configClause->isDoDedup() &&
                   configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_FLAT;

    if (SEARCH_PHASE_ONE == configClause->getPhaseNumber()) {
        vector<AggregateResults> aggResults;
        for (auto inputResult : inputResults) {
            aggResults.push_back(inputResult->getAggregateResults());
        }
        mergeAggResults(outputResult->getAggregateResults(), aggResults,
                        request->getAggregateClause(), request->getPool());
        mergeCoveredRanges(outputResult, inputResults);
        mergeGlobalVariables(outputResult, inputResults);
        mergeMatchDocs(outputResult, inputResults, request,
                       sorterWrapper, doDedup ,sl);
        mergePhaseOneSearchInfos(outputResult, inputResults);
    } else {
        mergeHits(outputResult, inputResults, doDedup);
        mergePhaseTwoSearchInfos(outputResult, inputResults);
    }
}

void Merger::mergePhaseOneSearchInfos(common::Result *outputResult,
                                      const vector<ResultPtr> &inputResults) {
    vector<PhaseOneSearchInfo *> inputSearchInfos;
    for (size_t i = 0; i < inputResults.size(); i++) {
        PhaseOneSearchInfo *searchInfo =
            inputResults[i]->getPhaseOneSearchInfo();
        if (searchInfo) {
            inputSearchInfos.push_back(searchInfo);
        }
    }
    if (inputSearchInfos.size() > 0) {
        PhaseOneSearchInfo *outputSearchInfo = new PhaseOneSearchInfo;
        outputSearchInfo->mergeFrom(inputSearchInfos);
        outputResult->setPhaseOneSearchInfo(outputSearchInfo);
    }
}

void Merger::mergePhaseTwoSearchInfos(common::Result *outputResult,
                                      const vector<ResultPtr> &inputResults) {
    vector<PhaseTwoSearchInfo *> inputSearchInfos;
    for (size_t i = 0; i < inputResults.size(); i++) {
        PhaseTwoSearchInfo *searchInfo =
            inputResults[i]->getPhaseTwoSearchInfo();
        if (searchInfo) {
            inputSearchInfos.push_back(searchInfo);
        }
    }
    if (inputSearchInfos.size() > 0) {
        PhaseTwoSearchInfo *outputSearchInfo = new PhaseTwoSearchInfo;
        outputSearchInfo->mergeFrom(inputSearchInfos);
        outputResult->setPhaseTwoSearchInfo(outputSearchInfo);
    }
}

void Merger::mergeMatchDocs(common::Result *outputResult,
                            const vector<ResultPtr> &inputResults,
                            const Request *request,
                            SorterWrapper *sorterWrapper,
                            bool doDedup,
                            SorterLocation sl) {
    uint32_t allMatchDocsSize = 0;
    const auto &noneEmptyResultPtr = findFirstNoneEmptyResult(inputResults,
            allMatchDocsSize);
    if (noneEmptyResultPtr == NULL) {
        AUTIL_LOG(DEBUG, "all the result is NULL");
        return;
    }
    auto pool = request->getPool();
    const auto &allocatorPtr = noneEmptyResultPtr->getMatchDocs()
                               ->getMatchDocAllocator();
    SimpleAttributeExpressionCreator exprCreator(pool, allocatorPtr.get());
    common::MultiErrorResultPtr errorResult;
    SortExpressionCreator sortExprCreator(NULL, NULL, NULL, errorResult, pool);
    ComparatorCreator compCreator(pool, false);
    DataProvider dataProvider;
    const auto *configClause = request->getConfigClause();
    auto requiredTopK = configClause->getStartOffset()
                        + configClause->getHitCount();
    SortParam sortParam(pool);
    sortParam.matchDocs.reserve(allMatchDocsSize);
    sortParam.requiredTopK = requiredTopK;
    auto meta = doMergeMatchDocsMeta(inputResults,
            doDedup, allocatorPtr, sortParam.matchDocs, sl);
    sortParam.totalMatchCount = meta.totalMatchDocs;
    sortParam.actualMatchCount = meta.actualMatchDocs;
    if (sortParam.matchDocs.size() > 0) {
        SorterResource sorterResource;
        const vector<AttributeItemMapPtr>& globalVariables = outputResult->getGlobalVarialbes();
        Tracer* requestTracer = outputResult->getTracer();
        REQUEST_TRACE_WITH_TRACER(requestTracer, TRACE3,
                "totalMatchDocs: %u, actualMatchDoc: %u",
                meta.totalMatchDocs, meta.actualMatchDocs);

        // sorter may update requiredTopK
        fillSorterResourceInfo(sorterResource, request, sortParam.requiredTopK,
                               &exprCreator, &sortExprCreator, allocatorPtr,
                               &dataProvider, &compCreator, &globalVariables, pool,
                               inputResults.size(), sl, requestTracer);

        sorter::SorterProvider sorterProvider(sorterResource);
        if (!sorterWrapper->beginSort(&sorterProvider)) {
            string errorMsg = "Sorter beginRequest error[" +
                              sorterProvider.getErrorMessage() + "]";
            AUTIL_LOG(WARN, "%s.", errorMsg.c_str());
            outputResult->getMultiErrorResult()->addError(
                    ERROR_SORTER_BEGIN_REQUEST_IN_MERGER, errorMsg);
            outputResult->setMatchDocs(new MatchDocs());
            return;
        }
        allocatorPtr->extend();
        if (allocatorPtr->hasSubDocAllocator()) {
            allocatorPtr->extendSub();
        }
        sorterProvider.setAggregateResultsPtr(
                AggregateResultsPtr(new AggregateResults(outputResult->getAggregateResults())));

        outputResult->setSortExprMeta(sorterProvider.getSortExprMeta());
        sorterWrapper->sort(sortParam);
        sorterWrapper->endSort();
    }

    MatchDocs *mergedMatchDocs = new MatchDocs();
    if (meta.errorCode != ERROR_NONE) {
        outputResult->getMultiErrorResult()->addError(meta.errorCode);
    }
    mergedMatchDocs->setTotalMatchDocs(sortParam.totalMatchCount);
    mergedMatchDocs->setActualMatchDocs(sortParam.actualMatchCount);

    uint32_t leftCount = min(sortParam.requiredTopK,
                             (uint32_t)sortParam.matchDocs.size());
    for (uint32_t i = 0; i < leftCount; i++) {
        mergedMatchDocs->addMatchDoc(sortParam.matchDocs[i]);
    }

    for (uint32_t i = leftCount; i < sortParam.matchDocs.size(); ++i) {
        allocatorPtr->deallocate(sortParam.matchDocs[i]);
    }
    mergedMatchDocs->setMatchDocAllocator(allocatorPtr);
    outputResult->setMatchDocs(mergedMatchDocs);
}

Merger::MergeMatchDocResultMeta Merger::doMergeMatchDocsMeta(
        const vector<ResultPtr> &inputResults,
        bool doDedup, const Ha3MatchDocAllocatorPtr &allocatorPtr,
        PoolVector<matchdoc::MatchDoc> &matchDocVect,
        SorterLocation sl)
{
    MergeMatchDocResultMeta meta;
    vector<MatchDocs*> multiMatchDocs;
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        MatchDocs *tmpMatchDocs = (*it)->getMatchDocs();
        if (NULL == tmpMatchDocs) {
            AUTIL_LOG(DEBUG, "empty MatchDocs");
            continue;
        }
        meta.totalMatchDocs += tmpMatchDocs->totalMatchDocs();
        meta.actualMatchDocs += tmpMatchDocs->actualMatchDocs();
        if (tmpMatchDocs->size() == 0) {
            AUTIL_LOG(DEBUG, "empty MatchDocs");
            continue;
        }
        const auto &tmpAllocatorPtr = tmpMatchDocs->getMatchDocAllocator();
        if (!tmpAllocatorPtr) {
            continue;
        }
        multiMatchDocs.push_back(tmpMatchDocs);
    }
    MatchDocDeduper deduper(allocatorPtr.get(), doDedup);
    meta.errorCode = deduper.dedup(multiMatchDocs, matchDocVect);
    return meta;
}

void Merger::mergeAggResults(AggregateResults &aggregateResults,
                             const vector<AggregateResults>& inputResults,
                             const AggregateClause *aggClause,
                             autil::mem_pool::Pool* pool)
{
    if (aggClause == NULL) {
        // no need to merge
        return;
    }
    bool checkRet = checkAggResults(aggClause, inputResults);
    if (!checkRet) {
        AUTIL_LOG(DEBUG, "checkAggResults error");
        return;
    }
    const AggregateDescriptions &des = aggClause->getAggDescriptions();
    AggResultSort aggResSort;
    for (size_t i = 0; i < des.size(); i++) {
        AggregateDescription *aggDesc = des[i];
        const string &groupExprStr = aggDesc->getGroupKeyExpr()->getExprString();
        uint32_t maxGroupCount = aggDesc->getMaxGroupCount();
        int32_t index = aggResSort.countFuncPostion(aggDesc);
        if (index > -1) {
            AggregateResultPtr aggResultPtr =
                mergeOneAggregateResult(inputResults, i, INT_MAX, groupExprStr, pool);
            assert(aggResultPtr);
            aggResSort.sortAggregateResult(aggResultPtr, maxGroupCount, index,
                    aggDesc->getSortType());
            aggregateResults.push_back(aggResultPtr);
        } else {
            AggregateResultPtr aggResultPtr =
                mergeOneAggregateResult(inputResults, i, maxGroupCount, groupExprStr, pool);
            aggregateResults.push_back(aggResultPtr);
        }
    }
}

AggregateResultPtr Merger::mergeOneAggregateResult(
        const vector<AggregateResults>& aggregateResults, size_t aggOffset,
        int32_t maxGroupCount, const string& groupExprStr, autil::mem_pool::Pool* pool)
{
    AggregateResultPtr aggResult;
    std::vector<matchdoc::MatchDoc> newDocs;
    for (vector<AggregateResults>::const_iterator it = aggregateResults.begin();
         it != aggregateResults.end(); ++it)
    {
        if (it->empty()) {
            continue;
        }
        const auto &tmpPtr = (*it)[aggOffset];
        assert(tmpPtr);
        if (tmpPtr->getGroupExprStr() != groupExprStr) {
            AUTIL_LOG(WARN, "aggregate key not match (%s != %s)",
                    tmpPtr->getGroupExprStr().c_str(), groupExprStr.c_str());
            continue;
        }
        const auto &docs = tmpPtr->getAggResultVector();
        if (aggResult) {
            const auto &baseAllocator = aggResult->getMatchDocAllocator();
            const auto &newAllocator = tmpPtr->getMatchDocAllocator();
            if (!baseAllocator->mergeAllocator(newAllocator.get(), docs, newDocs)) {
                continue;
            }
            tmpPtr->clearMatchdocs();
        } else {
            aggResult = tmpPtr;
            newDocs.insert(newDocs.end(), docs.begin(), docs.end());
        }
    }
    if (!aggResult) {
        AUTIL_LOG(INFO, "all the AggregateResult is NULL for this aggregator");
        return AggregateResultPtr();
    }
    if (newDocs.empty()) {
        return aggResult;
    }
    const auto &allocatorPtr = aggResult->getMatchDocAllocator();
    const auto &funNames = aggResult->getAggFunNames();
    auto groupKeyRef = allocatorPtr->findReferenceWithoutType(common::GROUP_KEY_REF);
    auto comp = VariableSlabComparator::createComparator(groupKeyRef);
    assert(comp.get());
    sort(newDocs.begin(), newDocs.end(), VariableSlabCompImpl(comp.get()));
    AggFunResultMergerPtr aggFunMergerPtr(
            createAggFunResultMerger(funNames, allocatorPtr));
    auto mergeIt = newDocs.begin();
    auto it = newDocs.begin();
    while (++it != newDocs.end()) {
        if (comp->compare(*mergeIt, *it)) {
            *(++mergeIt) = *it;
        } else {
            aggFunMergerPtr->merge(*mergeIt, *it, pool);
            allocatorPtr->deallocate(*it);
        }
    }
    newDocs.erase(++mergeIt, newDocs.end());
    aggResult->setAggResultVector(newDocs);
    return aggResult;
}

bool Merger::checkAggResults(const AggregateClause *aggClause,
                             const vector<AggregateResults>& results)
{
    bool allEmpty = true;
    bool checkRet = true;
    size_t expectAggResultSize = aggClause->getAggDescriptions().size();
    for (vector<AggregateResults>::const_iterator it = results.begin();
         it != results.end(); ++it)
    {
        auto &aggregateResults = *it;
        if (aggregateResults.size() > 0) {
            allEmpty = false;
        }
        checkRet &= checkAggResult(aggregateResults, expectAggResultSize);
    }
    return checkRet && !allEmpty;
}

bool Merger::checkAggResult(const AggregateResults &aggregateResults,
                            size_t expectSize)
{
    size_t actualAggSize = aggregateResults.size();
    if (0 != actualAggSize && expectSize != actualAggSize) {
        AUTIL_LOG(WARN, "the AggregateResult size is not right, "
                "expectSize:%lu, actualSize:%lu", expectSize, actualAggSize);
        return false;
    }
    return true;
}

void Merger::mergeCoveredRanges(common::Result *outputResult,
                                const vector<ResultPtr> &inputResults) {
    typedef common::Result::ClusterPartitionRanges ClusterPartitionRanges;
    typedef common::Result::PartitionRanges PartitionRanges;
    ClusterPartitionRanges tempRanges;
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        const ClusterPartitionRanges &ranges = (*it)->getCoveredRanges();
        for (ClusterPartitionRanges::const_iterator cIt = ranges.begin();
             cIt != ranges.end(); ++cIt)
        {
            const string &clusterName = cIt->first;
            const PartitionRanges &partRanges = cIt->second;
            tempRanges[clusterName].insert(tempRanges[clusterName].end(),
                    partRanges.begin(), partRanges.end());
        }
    }

    for (ClusterPartitionRanges::iterator it = tempRanges.begin();
         it != tempRanges.end(); ++it)
    {
        const string &clusterName = it->first;
        PartitionRanges &partRanges = it->second;
        sort(partRanges.begin(), partRanges.end());
        PartitionRanges::const_iterator curIt = partRanges.begin();
        PartitionRanges::const_iterator nxtIt;
        while (curIt != partRanges.end()) {
            uint32_t from = (*curIt).first;
            uint32_t to = (*curIt).second;
            nxtIt = curIt;
            ++nxtIt;
            while (nxtIt != partRanges.end() && (*nxtIt).first <= to + 1) {
                to = max(to, (*nxtIt).second);
                ++nxtIt;
            }
            outputResult->addCoveredRange(clusterName, from, to);
            curIt = nxtIt;
        }
    }
}

void Merger::mergeGlobalVariables(common::Result *outputResult,
                                  const vector<ResultPtr> &inputResults) {
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        outputResult->addGlobalVariables((*it)->getGlobalVarialbes());
    }
}

ResultPtr Merger::findFirstNoneEmptyResult(const vector<ResultPtr>& results,
        uint32_t &allMatchDocsSize)
{
    allMatchDocsSize = 0;
    ResultPtr resultPtr;
    for (vector<ResultPtr>::const_iterator it = results.begin();
         it != results.end(); ++it)
    {
        MatchDocs *matchDocs = (*it)->getMatchDocs();
        if (!matchDocs){
            continue;
        }
        allMatchDocsSize += matchDocs->size();
        if (resultPtr == NULL && NULL != matchDocs->getMatchDocAllocator()) {
            resultPtr = *it;
        }
    }
    return resultPtr;
}

void Merger::mergeHits(common::Result *outputResult,
                       const vector<ResultPtr> &inputResults,
                       bool doDedup) {
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        const ResultPtr &result = *it;
        Hits *tmpHits = result->getHits();
        if (!tmpHits) {
            if (result->hasError()) {
                AUTIL_LOG(WARN, "Find error [%s]",
                        (*it)->getMultiErrorResult()->toDebugString().c_str());
            }
            continue;
        }
        AUTIL_LOG(TRACE3, "Hits size[%u]", tmpHits->size());
        if (outputResult->getHits() == NULL) {
            outputResult->setHits(new Hits);
        }
        Hits *hits = outputResult->getHits();
        if (!hits->stealAndMergeHits(*tmpHits, doDedup)) {
            AUTIL_LOG(WARN, "Hits merge error");
        }
    }
}

void Merger::mergeErrors(common::Result *outputResult, const vector<ResultPtr> &inputResults) {
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        const ResultPtr &result = *it;
        if (result->hasError()) {
            outputResult->addErrorResult(result->getMultiErrorResult());
        }
    }
}

void Merger::mergeTracer(const Request *request,
                         common::Result *outputResult,
                         const vector<ResultPtr> &inputResults) {
    ConfigClause *configClause = request->getConfigClause();
    common::TracerPtr tracer(configClause->createRequestTracer("childnodes", ""));
    if (!tracer) {
        return;
    }
    vector<ResultPtr>::const_iterator it = inputResults.begin();
    for (; it != inputResults.end(); ++it)  {
        if (NULL == (*it)) {
            continue;
        }
        tracer->merge((*it)->getTracer());
    }
    outputResult->setTracer(tracer);
}

void Merger::mergeTracer(Tracer* tracer, const ResultPtr &resultPtr) {
    if (NULL != tracer && NULL != resultPtr) {
        tracer->merge(resultPtr->getTracer());
    }
}

AggFunResultMerger *Merger::createAggFunResultMerger(
        const vector<string> &funNames,
        const matchdoc::MatchDocAllocatorPtr &allocatorPtr)
{
    assert(allocatorPtr);
    auto referenceVec = allocatorPtr->getAllNeedSerializeReferences(
            SL_CACHE);
    assert(funNames.size() + 1 == referenceVec.size());

    auto aggFunResultMerger = new AggFunResultMerger();

    auto referIt = referenceVec.begin() + 1;
    auto funNameIt = funNames.begin();
    for (; referIt != referenceVec.end(); ++referIt, ++funNameIt) {
        auto aggFunMerger = AggFunMergerBase::createAggFunMerger(
                *funNameIt, *referIt);
        aggFunResultMerger->addAggFunMerger(aggFunMerger);
    }
    return aggFunResultMerger;
}

uint32_t Merger::mergeSummary(common::Result *outputResult,
                              const ResultPtrVector &inputResults,
                              bool allowLackOfSummary) {
    AUTIL_LOG(DEBUG, "Result size[%zd]", inputResults.size());
    vector<Hits *> hitsVec;
    hitsVec.reserve(inputResults.size());
    for (ResultPtrVector::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        ResultPtr distResultPtr = (*it);
        if (!distResultPtr) {
            ErrorResult errorResult(ERROR_QRS_SUMMARY_RESULT_NULL);
            outputResult->addErrorResult(errorResult);
            continue;
        }
        Hits *hits = distResultPtr->getHits();
        if (!hits) {
            AUTIL_LOG(WARN, "fillSummary result's hits is NULL!");
            continue;
        }
        hitsVec.push_back(hits);
    }
    Hits *hits = outputResult->getHits();
    assert(hits);
    uint32_t hitCount = hits->size();
    size_t lackCount = hits->stealSummary(hitsVec);

    if (lackCount > 0) {
        char errorMsg[200];
        sprintf(errorMsg, "The size of returned hits[%d] is not equal "
                "the size of required hits[%u] ",
                int32_t(hitCount - lackCount), hitCount);
        if (!allowLackOfSummary) {
            AUTIL_LOG(WARN, "%s", errorMsg);
        }
        ErrorResult errorResult(ERROR_QRS_SUMMARY_LACK, errorMsg);
        outputResult->addErrorResult(errorResult);
    }
    mergePhaseTwoSearchInfos(outputResult, inputResults);
    return (uint32_t)lackCount;
}

void Merger::fillSorterResourceInfo(SorterResource& sorterResource,
                                    const Request *request,
                                    uint32_t &topK,
                                    AttributeExpressionCreatorBase *exprCreator,
                                    SortExpressionCreator *sortExprCreator,
                                    const Ha3MatchDocAllocatorPtr &allocator,
                                    DataProvider *dataProvider,
                                    ComparatorCreator *compCreator,
                                    const vector<AttributeItemMapPtr> *globalVariables,
                                    Pool *pool,
                                    uint32_t resultSourceNum,
                                    SorterLocation sl,
                                    Tracer* requestTracer)
{
    auto scoreRef = allocator->findReference<score_t>(SCORE_REF);
    sorterResource.scoreExpression = NULL;
    if (scoreRef) {
        sorterResource.scoreExpression =
            AttributeExpressionFactory::createAttributeExpression(scoreRef, pool);
    }

    sorterResource.location = sl;
    sorterResource.pool = pool;
    sorterResource.request = request;
    sorterResource.attrExprCreator = exprCreator;
    sorterResource.sortExprCreator = sortExprCreator;
    sorterResource.dataProvider = dataProvider;
    sorterResource.matchDocAllocator = allocator;
    sorterResource.fieldInfos = NULL;
    sorterResource.requiredTopk = &topK;
    sorterResource.resultSourceNum = resultSourceNum;
    sorterResource.comparatorCreator = compCreator;
    sorterResource.globalVariables = globalVariables;
    sorterResource.requestTracer = requestTracer;
}

} // namespace proxy
} // namespace isearch
