#include <ha3/turing/ops/Ha3ResultMergeOp.h>
#include <ha3/proxy/Merger.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(proxy);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(turing);

void Ha3ResultMergeOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);


    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    RequestPtr request = ha3RequestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    auto resultsTensor = ctx->input(1).flat<Variant>();
    auto resultCount = ctx->input(1).dim_size(0);
    vector<ResultPtr> inputResults;
    for (auto i = 0; i < resultCount; i++) {
        auto ha3ResultVariant = resultsTensor(i).get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));
        inputResults.push_back(result);
    }

    ResultPtr outputResult(new Result);
    mergeResults(outputResult.get(), inputResults, request.get());

     if (queryResource->getTracer()) {
        queryResource->getTracer()->merge(outputResult->getTracer());
        outputResult->setTracer(queryResource->getTracerPtr());
     }

    outputResult->setSrcCount(inputResults.size());

    bool hasMatchDoc = judgeHasMatchDoc(outputResult);
    int32_t outPort = hasMatchDoc? 0:1;
    auto pool = queryResource->getPool();
    Ha3ResultVariant ha3Result(outputResult, pool);
    //Tensor* out = nullptr;
    auto resultTensor = Tensor(DT_VARIANT, TensorShape({}));
    resultTensor.scalar<Variant>()() = ha3Result;
    ctx->set_output(outPort, resultTensor);
    //OP_REQUIRES_OK(ctx, ctx->allocate_output(outPort, {}, &out));
}

void Ha3ResultMergeOp::mergeResults(Result *outputResult,
                  const vector<ResultPtr> &inputResults,
                  const Request* request)
{
    mergeErrors(outputResult, inputResults);
    mergeTracer(request, outputResult, inputResults);

    ConfigClause *configClause = request->getConfigClause();
    assert(configClause);
    // can not dedup in sub_doc_flat mode,
    bool doDedup = configClause->isDoDedup() &&
                   configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_FLAT;

    if (SEARCH_PHASE_ONE == configClause->getPhaseNumber()) {
        mergeAggResults(outputResult, inputResults, request);
        mergeCoveredRanges(outputResult, inputResults);
        mergeGlobalVariables(outputResult, inputResults);
        mergeMatchDocs(outputResult, inputResults, request, doDedup);
        mergePhaseOneSearchInfos(outputResult, inputResults);
    } else {
        mergeHits(outputResult, inputResults, doDedup);
        mergePhaseTwoSearchInfos(outputResult, inputResults);
    }

}

void Ha3ResultMergeOp::mergeErrors(Result *outputResult,
                 const vector<ResultPtr> &inputResults)
{
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        const ResultPtr &result = *it;
        if (result->hasError()) {
            outputResult->addErrorResult(result->getMultiErrorResult());
        }
    }
}

void Ha3ResultMergeOp::mergeTracer(const Request *request,
                 Result *outputResult,
                 const vector<ResultPtr> &inputResults)
{
    ConfigClause *configClause = request->getConfigClause();
    common::Tracer *tracer = configClause->createRequestTracer("childnodes", "");
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
    outputResult->setTracer(TracerPtr(tracer));
}

AggFunResultMerger* Ha3ResultMergeOp::createAggFunResultMerger(
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

AggregateResultPtr Ha3ResultMergeOp::mergeOneAggregateResult(
    const vector<ResultPtr> &results, size_t aggOffset,
    const string &groupExprStr, autil::mem_pool::Pool *pool) {
    AggregateResultPtr aggResult;
    std::vector<matchdoc::MatchDoc> newDocs;
    for (vector<ResultPtr>::const_iterator it = results.begin();
         it != results.end(); ++it)
    {
        if ((*it)->getAggregateResultCount() == 0) {
            continue;
        }
        const auto &tmpPtr = (*it)->getAggregateResult(aggOffset);
        assert(tmpPtr);
        if (tmpPtr->getGroupExprStr() != groupExprStr) {
            HA3_LOG(WARN, "aggregate key not match (%s != %s)",
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
        HA3_LOG(INFO, "all the AggregateResult is NULL for this aggregator");
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

bool Ha3ResultMergeOp::checkAggResults(const AggregateClause *aggClause,
                     const vector<ResultPtr>& results)
{
    bool allEmpty = true;
    bool checkRet = true;
    size_t expectAggResultSize = aggClause->getAggDescriptions().size();
    for (vector<ResultPtr>::const_iterator it = results.begin();
         it != results.end(); ++it)
    {
        if ((*it)->getAggregateResults().size() > 0) {
            allEmpty = false;
        }
        checkRet &= checkAggResult(*it, expectAggResultSize);
    }
    return checkRet && !allEmpty;
}

bool Ha3ResultMergeOp::checkAggResult(const ResultPtr &resultPtr,
                    size_t expectSize)
{
    size_t actualAggSize = resultPtr->getAggregateResults().size();
    if (0 != actualAggSize && expectSize != actualAggSize) {
        HA3_LOG(WARN, "the AggregateResult size is not right, "
                "expectSize:%lu, actualSize:%lu", expectSize, actualAggSize);
        return false;
    }
    return true;
}

void Ha3ResultMergeOp::mergeAggResults(Result *outputResult,
                     const vector<ResultPtr>& inputResults,
                     const Request *request)
{
    const AggregateClause *aggClause = request->getAggregateClause();
    if (aggClause == NULL) {
        // no need to merge
        return;
    }
    bool checkRet = checkAggResults(aggClause, inputResults);
    if (!checkRet) {
        HA3_LOG(DEBUG, "checkAggResults error");
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
                mergeOneAggregateResult(inputResults, i, groupExprStr, request->getPool());
            assert(aggResultPtr);
            aggResSort.collectAggregateResult(aggResultPtr,
                    maxGroupCount, index);
            outputResult->addAggregateResult(aggResultPtr);
        } else {
            outputResult->addAggregateResult(
                    mergeOneAggregateResult(inputResults, i, groupExprStr, request->getPool()));
        }
    }
}

void Ha3ResultMergeOp::mergeCoveredRanges(Result *outputResult,
                        const vector<ResultPtr> &inputResults)
{
    typedef Result::ClusterPartitionRanges ClusterPartitionRanges;
    typedef Result::PartitionRanges PartitionRanges;
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

void Ha3ResultMergeOp::mergeGlobalVariables(Result *outputResult,
                          const vector<ResultPtr> &inputResults)
{
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        outputResult->addGlobalVariables((*it)->getGlobalVarialbes());
    }
}

ResultPtr Ha3ResultMergeOp::findFirstNoneEmptyResult(const vector<ResultPtr>& results,
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

Ha3ResultMergeOp::MergeMatchDocResultMeta Ha3ResultMergeOp::doMergeMatchDocsMeta(
        const vector<ResultPtr> &inputResults,
        bool doDedup, const Ha3MatchDocAllocatorPtr &allocatorPtr,
        PoolVector<matchdoc::MatchDoc> &matchDocVect)
{
    MergeMatchDocResultMeta meta;
    vector<MatchDocs*> multiMatchDocs;
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        MatchDocs *tmpMatchDocs = (*it)->getMatchDocs();
        if (NULL == tmpMatchDocs) {
            HA3_LOG(DEBUG, "empty MatchDocs");
            continue;
        }
        meta.totalMatchDocs += tmpMatchDocs->totalMatchDocs();
        meta.actualMatchDocs += tmpMatchDocs->actualMatchDocs();
        if (tmpMatchDocs->size() == 0) {
            HA3_LOG(DEBUG, "empty MatchDocs");
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

void Ha3ResultMergeOp::mergeMatchDocs(Result *outputResult,
                    const vector<ResultPtr> &inputResults,
                    const Request *request,
                    bool doDedup)
{
    uint32_t allMatchDocsSize = 0;
    const auto &noneEmptyResultPtr = findFirstNoneEmptyResult(inputResults,
            allMatchDocsSize);
    if (noneEmptyResultPtr == NULL) {
        HA3_LOG(DEBUG, "all the result is NULL");
        return;
    }
    auto pool = request->getPool();
    const auto &allocatorPtr = noneEmptyResultPtr->getMatchDocs()
                               ->getMatchDocAllocator();
    common::MultiErrorResultPtr errorResult;
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> poolMatchDocs(pool);

    poolMatchDocs.reserve(allMatchDocsSize);
    auto meta = doMergeMatchDocsMeta(inputResults,
            doDedup, allocatorPtr, poolMatchDocs);

    MatchDocs *mergedMatchDocs = new MatchDocs();
    HA3_LOG(TRACE3, "totalMatchDocs: %u, actualMatchDoc: %u",
            meta.totalMatchDocs, meta.actualMatchDocs);
    if (meta.errorCode != ERROR_NONE) {
        outputResult->getMultiErrorResult()->addError(meta.errorCode);
    }
    mergedMatchDocs->setTotalMatchDocs(meta.totalMatchDocs);
    mergedMatchDocs->setActualMatchDocs(meta.actualMatchDocs);
    for (uint32_t i = 0; i < poolMatchDocs.size(); i++) {
        mergedMatchDocs->addMatchDoc(poolMatchDocs[i]);
    }
    mergedMatchDocs->setMatchDocAllocator(allocatorPtr);
    outputResult->setMatchDocs(mergedMatchDocs);
}

void Ha3ResultMergeOp::mergePhaseOneSearchInfos(Result *outputResult,
                              const vector<ResultPtr> &inputResults)
{
    vector<PhaseOneSearchInfo *> inputSearchInfos;
    bool useTruncateOpt = false;
    for (size_t i = 0; i < inputResults.size(); i++) {
        useTruncateOpt |= inputResults[i]->useTruncateOptimizer();
        PhaseOneSearchInfo *searchInfo = inputResults[i]->getPhaseOneSearchInfo();
        if (searchInfo) {
            inputSearchInfos.push_back(searchInfo);
        }
    }
    outputResult->setUseTruncateOptimizer(useTruncateOpt);
    if (inputSearchInfos.size() > 0) {
        PhaseOneSearchInfo *outputSearchInfo = new PhaseOneSearchInfo;
        outputSearchInfo->mergeFrom(inputSearchInfos);
        outputResult->setPhaseOneSearchInfo(outputSearchInfo);
    }
}

void Ha3ResultMergeOp::mergeHits(Result *outputResult,
               const vector<ResultPtr> &inputResults,
               bool doDedup)
{
    for (vector<ResultPtr>::const_iterator it = inputResults.begin();
         it != inputResults.end(); ++it)
    {
        const ResultPtr &result = *it;
        Hits *tmpHits = result->getHits();
        if (!tmpHits) {
            if (result->hasError()) {
                HA3_LOG(WARN, "Find error [%s]",
                        (*it)->getMultiErrorResult()->toDebugString().c_str());
            }
            continue;
        }
        HA3_LOG(TRACE3, "Hits size[%u]", tmpHits->size());
        if (outputResult->getHits() == NULL) {
            outputResult->setHits(new Hits);
        }
        Hits *hits = outputResult->getHits();
        if (!hits->stealAndMergeHits(*tmpHits, doDedup)) {
            HA3_LOG(WARN, "Hits merge error");
        }
    }
}


void Ha3ResultMergeOp::mergePhaseTwoSearchInfos(Result *outputResult,
                              const vector<ResultPtr> &inputResults)
{
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

bool Ha3ResultMergeOp::judgeHasMatchDoc(const ResultPtr &outputResult) {
    if (NULL != outputResult->getMatchDocs() && outputResult->getMatchDocs()->size() > 0) {
        return true;
    }
    return false;
}

HA3_LOG_SETUP(turing, Ha3ResultMergeOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultMergeOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultMergeOp);

END_HA3_NAMESPACE(turing);
