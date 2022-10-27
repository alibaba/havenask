#include "ha3/turing/ops/Ha3SeekParaMergeOp.h"
#include "ha3/turing/ops/Ha3ResourceUtil.h"
#include "ha3/proxy/Merger.h"
#include <ha3/rank/RankProfileManager.h>
#include <ha3/turing/variant/AggregateResultsVariant.h>

using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(proxy);
USE_HA3_NAMESPACE(service);

BEGIN_HA3_NAMESPACE(turing);

void Ha3SeekParaMergeOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    RequestPtr request = ha3RequestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    SearcherSessionResource *searcherSessionResource =
        dynamic_cast<SearcherSessionResource *>(sessionResource);
    OP_REQUIRES(ctx, searcherSessionResource,
                errors::Unavailable("ha3 searcher session resource unavailable"));

    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherQueryResource,
                errors::Unavailable("ha3 searcher query resource unavailable"));
    auto pool = searcherQueryResource->getPool();
    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    if (metricsCollector) {
        metricsCollector->parallelSeekEndTrigger();
        metricsCollector->parallelMergeStartTrigger();
    }
    // read input resources
    vector<SeekResourcePtr> seekResources;
    readSeekResourcesFromInput(ctx, seekResources);
    vector<SearchCommonResourcePtr> commonResources;
    for (auto seekResource: seekResources) {
        OP_REQUIRES(ctx, seekResource,
                    errors::Unavailable("invalid seek resource"));
        auto commonResource = seekResource->commonResource;
        OP_REQUIRES(ctx, commonResource,
                    errors::Unavailable("invalid common resource"));
        commonResources.push_back(commonResource);
        auto partitionResource = seekResource->partitionResource;
        OP_REQUIRES(ctx, partitionResource,
                    errors::Unavailable("invalid partition resource"));
        searcherQueryResource->paraSeekReaderWrapperVec.push_back(
                partitionResource->indexPartitionReaderWrapper);
    }

    // read hasErrors flag
    vector<bool> hasErrors;
    readHasErrorsFromInput(ctx, hasErrors);

    // read input inner results
    vector<HA3_NS(search)::InnerSearchResult> seekResults;
    readSeekResultsFromInput(ctx, seekResults);

    const vector<InnerSearchResult> &innerResults =
        filterErrorResults(hasErrors, seekResults);

    if (0 == innerResults.size()) {
        ResultPtr errorResult = constructErrorResult(commonResources,
                searcherQueryResource);
        Ha3ResultVariant ha3ErrorResultVariant(errorResult, pool);
        tensorflow::Tensor* errorResultTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(5, {}, &errorResultTensor));
        errorResultTensor->scalar<tensorflow::Variant>()() = ha3ErrorResultVariant;
        return;
    }

// 1. merge matchdocs, merge allocator
    InnerSearchResult mergedInnerResult(pool);
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> poolMatchDocs(pool);
    ErrorCode mergeErrorCode = ERROR_NONE;
    bool ret = mergeMatchDocs(innerResults, mergedInnerResult, mergeErrorCode);
    OP_REQUIRES(ctx, ret, errors::Unavailable("merge inner results failed"));

// 2. merge agg
    auto aggClause = request->getAggregateClause();
    mergedInnerResult.aggResultsPtr.reset(new AggregateResults());
    AggregateResults &aggregateResults = *mergedInnerResult.aggResultsPtr;
    mergeAggResults(aggregateResults, innerResults, aggClause, pool);

// 3. prepare resources
    prepareResource(ctx, request, seekResources,
                    mergedInnerResult.matchDocAllocatorPtr,
                    searcherQueryResource);

// 4. merge tracer
    auto tracer = searcherQueryResource->getTracer();
    mergeTracer(commonResources, tracer);

// 5. merge errors
    mergeErrors(commonResources, searcherQueryResource->commonResource);

// 6. merge global variable manages
    mergeGlobalVariableManagers(commonResources,
                               searcherQueryResource->commonResource);

// 7. merge metrics
    mergeMetrics(commonResources, queryResource->getQueryMetricsReporter(),
                 searcherQueryResource->commonResource);

//8. prepare output
    suez::turing::MatchDocsVariant matchDocsVariant(
            mergedInnerResult.matchDocAllocatorPtr, pool);
    std::vector<matchdoc::MatchDoc> matchDocVec;
    matchDocVec.insert(matchDocVec.begin(),
                       mergedInnerResult.matchDocVec.begin(),
                       mergedInnerResult.matchDocVec.end());
    matchDocsVariant.stealMatchDocs(matchDocVec);

    tensorflow::Tensor* matchDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocs));
    matchDocs->scalar<tensorflow::Variant>()() = matchDocsVariant;

    tensorflow::Tensor* extraDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &extraDocs));
    extraDocs->scalar<uint32_t>()() = mergedInnerResult.extraRankCount;

    tensorflow::Tensor* totalDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &totalDocs));
    totalDocs->scalar<uint32_t>()() = mergedInnerResult.totalMatchDocs;

    tensorflow::Tensor* actualDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(3, {}, &actualDocs));
    actualDocs->scalar<uint32_t>()() = mergedInnerResult.actualMatchDocs;


    AggregateResultsVariant ha3AggResultsVariant(
            mergedInnerResult.aggResultsPtr,  pool);
    tensorflow::Tensor* aggResults = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(4, {}, &aggResults));
    aggResults->scalar<tensorflow::Variant>()() = ha3AggResultsVariant;
    if (metricsCollector) {
        metricsCollector->parallelMergeEndTrigger();
    }
    _tags.AddTag("way_count", StringUtil::toString(_wayCount));
    ADD_OP_BASE_TAG(_tags);
}

void Ha3SeekParaMergeOp::prepareResource(
        tensorflow::OpKernelContext *ctx,
        const RequestPtr request,
        const vector<SeekResourcePtr> &seekResources,
        const Ha3MatchDocAllocatorPtr &matchDocAllocatorPtr,
        SearcherQueryResource *searcherQueryResource) {
    int32_t reserveIndex = -1;
    for (size_t i=0; i<seekResources.size(); i++) {
        auto &tmpAllocatorPtr
            = seekResources[i]->commonResource->matchDocAllocator;
        if (tmpAllocatorPtr && matchDocAllocatorPtr == tmpAllocatorPtr) {
            reserveIndex = (int32_t)i;
            break;
        }
    }
    OP_REQUIRES(ctx, reserveIndex != -1,
                errors::Unavailable("prepareResource error"));
    auto &reservedSeekResource = seekResources[reserveIndex];
    SearchCommonResourcePtr commonResourcePtr = createCommonResource(
            request, matchDocAllocatorPtr, searcherQueryResource,
            reservedSeekResource->commonResource);
    searcherQueryResource->commonResource = commonResourcePtr;

    searcherQueryResource->partitionResource =
        reservedSeekResource->partitionResource;
    auto &functionProvider =
        searcherQueryResource->partitionResource->functionProvider;
    functionProvider.setRequestTracer(commonResourcePtr->tracer);
    functionProvider.setQueryTerminator(commonResourcePtr->timeoutTerminator);

    searcherQueryResource->runtimeResource = reservedSeekResource->runtimeResource;
}

bool Ha3SeekParaMergeOp::mergeMatchDocs(
        const vector<InnerSearchResult> &innerResults,
        InnerSearchResult &mergedInnerResult,
        ErrorCode &errorCode)
{
    mergedInnerResult.totalMatchDocs = 0;
    mergedInnerResult.actualMatchDocs = 0;
    mergedInnerResult.extraRankCount = 0;
    auto resultIndex = findFirstNoneEmptyResult(innerResults);
    if (-1 == resultIndex || resultIndex >= (int32_t)innerResults.size()) {
        return false;
    }
    vector<shared_ptr<MatchDocs>> tmpMatchDocs;
    for (auto &innerResult: innerResults) {
        mergedInnerResult.totalMatchDocs += innerResult.totalMatchDocs;
        mergedInnerResult.actualMatchDocs += innerResult.actualMatchDocs;
        mergedInnerResult.extraRankCount += innerResult.extraRankCount;
        tmpMatchDocs.push_back(
                shared_ptr<MatchDocs> (createMatchDocs(innerResult)));
    }
    auto &firstInnerResult = innerResults[resultIndex];
    auto allocatorPtr = firstInnerResult.matchDocAllocatorPtr;
    errorCode = doMergeMatchDocs(tmpMatchDocs, allocatorPtr,
                                 mergedInnerResult.matchDocVec);
    mergedInnerResult.matchDocAllocatorPtr = allocatorPtr;
    return true;
}

void Ha3SeekParaMergeOp::mergeAggResults(
        AggregateResults &outAggregateResults,
        const vector<InnerSearchResult> &innerResults,
        const AggregateClause *aggClause,
        autil::mem_pool::Pool *pool)
{
    vector<AggregateResults> inputAggResults;
    for (const auto &result : innerResults) {
        if (result.aggResultsPtr) {
            inputAggResults.push_back(*result.aggResultsPtr);
        }
    }
    Merger::mergeAggResults(outAggregateResults, inputAggResults, aggClause, pool);
}


ErrorCode Ha3SeekParaMergeOp::doMergeMatchDocs(
        const std::vector<std::shared_ptr<MatchDocs>> &inputMatchDocsVec,
        const Ha3MatchDocAllocatorPtr &allocatorPtr,
        PoolVector<matchdoc::MatchDoc> &outMatchDocVect)
{
    vector<MatchDocs*> multiMatchDocs;
    for (auto &tmpMatchDocs: inputMatchDocsVec)
    {
        if (NULL == tmpMatchDocs) {
            HA3_LOG(DEBUG, "empty MatchDocs");
            continue;
        }
        if (tmpMatchDocs->size() == 0) {
            HA3_LOG(DEBUG, "empty MatchDocs");
            continue;
        }
        const auto &tmpAllocatorPtr = tmpMatchDocs->getMatchDocAllocator();
        if (!tmpAllocatorPtr) {
            continue;
        }
        multiMatchDocs.push_back(tmpMatchDocs.get());
    }
    MatchDocDeduper deduper(allocatorPtr.get(), false, true);
    ErrorCode errorCode = deduper.dedup(multiMatchDocs, outMatchDocVect);
    return errorCode;
}

MatchDocs *Ha3SeekParaMergeOp::createMatchDocs(
        const InnerSearchResult &innerResult) {
    MatchDocs *matchDocs = new MatchDocs();
    matchDocs->setTotalMatchDocs(innerResult.totalMatchDocs);
    matchDocs->setActualMatchDocs(innerResult.actualMatchDocs);
    matchDocs->setMatchDocAllocator(innerResult.matchDocAllocatorPtr);
    matchDocs->resize(innerResult.matchDocVec.size());
    HA3_LOG(TRACE3, "matchDocVect.size()=[%zd] in Ha3SeekParaMergerOp",
            innerResult.matchDocVec.size());
    for (uint32_t i = 0; i < innerResult.matchDocVec.size(); i++) {
        matchDocs->insertMatchDoc(i, innerResult.matchDocVec[i]);
    }
    return matchDocs;
}

ResultPtr Ha3SeekParaMergeOp::constructErrorResult(
        vector<SearchCommonResourcePtr> &commonResources,
        SearcherQueryResource *searcherQueryResource)
{
    ResultPtr result = ResultPtr(new Result(new MatchDocs()));
    auto tracer = searcherQueryResource->getTracerPtr();
    for (const auto &resource : commonResources) {
        result->addErrorResult(resource->errorResult);
        if (tracer) {
            tracer->merge(resource->tracer);
        }
    }
    result->setTracer(tracer);
    return result;
}

int32_t Ha3SeekParaMergeOp::findFirstNoneEmptyResult(
        const vector<InnerSearchResult>& innerResults)
{
    int32_t maxDocCount = -1;
    int32_t resultIndex = -1;
    for (size_t index = 0; index < innerResults.size(); ++index) {
        auto &innerResult = innerResults[index];
        if (NULL != innerResult.matchDocAllocatorPtr) {
            if (maxDocCount < (int32_t) innerResult.matchDocVec.size()) {
                maxDocCount = (int32_t) innerResult.matchDocVec.size();
                resultIndex = index;
                if (maxDocCount > 0) {
                    break;
                }
            }
        }
    }
    return resultIndex;
}

vector<InnerSearchResult>  Ha3SeekParaMergeOp::filterErrorResults(
        const vector<bool> &hasErrors,
        const vector<InnerSearchResult> &inputResults) {
    vector<InnerSearchResult> results;
    if (hasErrors.size() != inputResults.size()) {
        return results;
    }
    for (size_t i = 0; i < inputResults.size(); i++) {
        if (!hasErrors[i]) {
            results.push_back(inputResults[i]);
        }
    }
    return results;
}

bool Ha3SeekParaMergeOp::doRank(const SearchCommonResourcePtr &commonResource,
                                const SearchPartitionResourcePtr &partitionResource,
                                const SearchProcessorResource *processorResource,
                                const RequestPtr &request,
                                InnerSearchResult &innerResult)
{
    HitCollectorManagerPtr hitCollectorManager(createHitCollectorManager(
                    commonResource, partitionResource, processorResource));
    if (!hitCollectorManager
        || !hitCollectorManager->init(request.get(),
                processorResource->docCountLimits.runtimeTopK))
    {
        HA3_LOG(WARN, "init HitCollectorManager failed!");
        return false;
    }
    rank::HitCollectorBase *rankCollector =
        hitCollectorManager->getRankHitCollector();
    bool needFlatten = commonResource->matchDocAllocator->hasSubDocAllocator() &&
                       request->getConfigClause()->getSubDocDisplayType() ==
                       SUB_DOC_DISPLAY_FLAT;
    rankCollector->updateExprEvaluatedStatus();
    for (const auto &matchDoc : innerResult.matchDocVec) {
        rankCollector->collect(matchDoc, needFlatten);
    }
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> target(request->getPool());
    rankCollector->stealAllMatchDocs(target);
    innerResult.matchDocVec.swap(target);
    return true;
}

HitCollectorManager *Ha3SeekParaMergeOp::createHitCollectorManager(
        const SearchCommonResourcePtr &commonResource,
        const SearchPartitionResourcePtr &partitionResource,
        const SearchProcessorResource *processorResource) const
{
    return new HitCollectorManager(
            partitionResource->attributeExpressionCreator.get(),
            processorResource->sortExpressionCreator,
            commonResource->pool,
            processorResource->comparatorCreator,
            commonResource->matchDocAllocator,
            commonResource->errorResult);
}

void Ha3SeekParaMergeOp::readSeekResourcesFromInput(
        tensorflow::OpKernelContext* ctx,
        std::vector<SeekResourcePtr> &seekResources)
{
    OpInputList seekResourcesInputList;
    OP_REQUIRES_OK(ctx, ctx->input_list("seek_resources", &seekResourcesInputList));
    OP_REQUIRES(ctx, _wayCount == seekResourcesInputList.size(),
                errors::Unavailable("input seek resources not match way count"));
    for (int i = 0; i < _wayCount; i++) {
        auto seekResourceVariant =
            seekResourcesInputList[i].scalar<Variant>()().get<SeekResourceVariant>();
        OP_REQUIRES(ctx, seekResourceVariant,
                    errors::Unavailable("invalid seek resource variant"));
        auto seekResource = seekResourceVariant->getSeekResource();
        seekResources.push_back(seekResource);
    }
}

void Ha3SeekParaMergeOp::readHasErrorsFromInput(
        tensorflow::OpKernelContext* ctx,
        std::vector<bool> &hasErrors)
{
    OpInputList hasErrorsInputList;
    OP_REQUIRES_OK(ctx, ctx->input_list("has_errors", &hasErrorsInputList));
    OP_REQUIRES(ctx, _wayCount == hasErrorsInputList.size(),
                errors::Unavailable("input hasErrors not match way count"));
    for (int i = 0; i < _wayCount; i++) {
        auto hasError = hasErrorsInputList[i].scalar<bool>()();
        hasErrors.push_back(hasError);
    }
}

void Ha3SeekParaMergeOp::readSeekResultsFromInput(
        tensorflow::OpKernelContext* ctx,
        vector<InnerSearchResult> &seekResults)
{
    OpInputList seekResultsInputList;
    OP_REQUIRES_OK(ctx, ctx->input_list("seek_results", &seekResultsInputList));
    OP_REQUIRES(ctx, _wayCount == seekResultsInputList.size(),
                errors::Unavailable("input seek results not match way count"));

    for (int i = 0; i < _wayCount; i++) {
        auto seekResultVariant =
            seekResultsInputList[i].scalar<Variant>()().get<InnerResultVariant>();
        OP_REQUIRES(ctx, seekResultVariant,
                    errors::Unavailable("invalid seek result variant"));
        auto seekResult = seekResultVariant->getInnerResult();
        seekResults.push_back(seekResult);
    }
}

SearchCommonResourcePtr Ha3SeekParaMergeOp::createCommonResource(
        const RequestPtr &request,
        const Ha3MatchDocAllocatorPtr &matchDocAllocator,
        const SearcherQueryResource *searcherQueryResource,
        const SearchCommonResourcePtr &reservedCommonResource)
{
    SearchCommonResourcePtr commonResource(new SearchCommonResource(
                    searcherQueryResource->getPool(),
                    reservedCommonResource->tableInfoPtr,
                    searcherQueryResource->sessionMetricsCollector,
                    searcherQueryResource->getSeekTimeoutTerminator(),
                    searcherQueryResource->getTracer(),
                    reservedCommonResource->functionCreator,
                    reservedCommonResource->cavaPluginManagerPtr,
                    request.get(),
                    reservedCommonResource->cavaAllocator,
                    reservedCommonResource->cavaJitModules,
                    matchDocAllocator));
    if (!commonResource) {
        return commonResource;
    }
    commonResource->matchDocAllocator = matchDocAllocator;
    return commonResource;
}

void Ha3SeekParaMergeOp::mergeTracer(
        const vector<SearchCommonResourcePtr> &commonResources,
        Tracer *tracer)
{
    if (!tracer) {
        return;
    }
    for (auto &resource : commonResources) {
        tracer->merge(resource->tracer);
    }
}

void Ha3SeekParaMergeOp::mergeGlobalVariableManagers(
        const vector<SearchCommonResourcePtr> &commonResources,
        SearchCommonResourcePtr &outCommonResource)
{
    if (!outCommonResource) {
        return;
    }
    for (auto &resource : commonResources) {
        const auto &globalVarMgr =
            resource->dataProvider.getGlobalVariableManager();
        outCommonResource->paraGlobalVariableManagers.push_back(globalVarMgr);
    }
}

void Ha3SeekParaMergeOp::mergeErrors(
        const vector<SearchCommonResourcePtr> &commonResources,
        SearchCommonResourcePtr &outCommonResource)
{
    if (!outCommonResource) {
        return;
    }
    for (auto &resource : commonResources) {
        outCommonResource->errorResult->addErrorResult(resource->errorResult);
    }
}

void Ha3SeekParaMergeOp::mergeMetrics(
        const vector<SearchCommonResourcePtr> &commonResources,
        kmonitor::MetricsReporter *userMetricReporter,
        SearchCommonResourcePtr &outCommonResource)
{
    if (!userMetricReporter || commonResources.size() == 0) {
        return;
    }
    string amonPath("phase1.para.");
    int32_t idx = 0;
    int32_t seekCount = 0, matchCount = 0, seekDocCount = 0, rerankcount = 0;
    int32_t strongJoinFilterCount = 0, estimatedMatchCount = 0, aggregateCount = 0;
    bool useTruncateOptimizer = false;
    for (auto &resource : commonResources) {
        auto collector = resource->sessionMetricsCollector;
        if (!collector) {
            continue;
        }
        string path = amonPath;
        auto tags = _tags;
        tags.AddTag("index", StringUtil::toString(idx++));
        // latency
        userMetricReporter->report(collector->getBeforeSearchLatency(),
                path + "beforeSearchLatency", kmonitor::GAUGE, &_tags);
        userMetricReporter->report(collector->getRankLatency(),
                path + "rankLatency", kmonitor::GAUGE, &tags);
        userMetricReporter->report(collector->getReRankLatency(),
                path + "reranklatency", kmonitor::GAUGE, &tags);

        // count
        auto paraSeekCount = collector->getSeekCount();
        userMetricReporter->report(paraSeekCount,
                path + "seekCount", kmonitor::GAUGE, &tags);
        seekCount += paraSeekCount != -1 ? paraSeekCount : 0;
        auto paraMatchCount = collector->getMatchCount();
        userMetricReporter->report(paraMatchCount,
                path + "matchCount", kmonitor::GAUGE, &tags);
        matchCount += paraMatchCount != -1 ? paraMatchCount : 0;
        userMetricReporter->report(collector->getSeekDocCount(),
                path + "seekDocCount", kmonitor::GAUGE, &tags);
        seekDocCount += collector->getSeekDocCount();
        auto paraRerankCount = collector->getReRankCount();
        userMetricReporter->report(paraRerankCount,
                path + "reRankCount", kmonitor::GAUGE, &tags);
        rerankcount += paraRerankCount != -1 ? paraRerankCount : 0;
        auto paraStrongCount = collector->getStrongJoinFilterCount();
        userMetricReporter->report(paraStrongCount,
                path + "strongJoinFilterCount", kmonitor::GAUGE, &tags);
        strongJoinFilterCount += paraStrongCount != -1 ? paraStrongCount : 0;
        auto paraEstimateMcount = collector->getEstimatedMatchCount();
        userMetricReporter->report(paraEstimateMcount,
                path + "estimatedMatchCount", kmonitor::GAUGE, &tags);
        estimatedMatchCount += paraEstimateMcount != -1 ? paraEstimateMcount : 0;
        auto paraAggCount = collector->getAggregateCount();
        userMetricReporter->report(paraAggCount,
                path + "aggregateCount", kmonitor::GAUGE, &tags);
        aggregateCount += paraAggCount != -1 ? paraAggCount : 0;

        // debug
        if (collector->useTruncateOptimizer()) {
            useTruncateOptimizer = true;
            userMetricReporter->report(1,
                    path + "debug/useTruncateOptimizerNum", kmonitor::QPS, &tags);
        }
    }

    if(!outCommonResource || !outCommonResource->sessionMetricsCollector) {
        return;
    }
    seekCount = seekCount ? seekCount : -1;
    matchCount = matchCount ? matchCount : -1;
    strongJoinFilterCount = strongJoinFilterCount ? strongJoinFilterCount : -1;
    estimatedMatchCount = estimatedMatchCount ? estimatedMatchCount : -1;
    aggregateCount = aggregateCount ? aggregateCount : -1;
    rerankcount = rerankcount ? rerankcount : -1;
    const auto &outCollector = outCommonResource->sessionMetricsCollector;
    outCollector->seekCountTrigger(seekCount);
    outCollector->matchCountTrigger(matchCount);
    outCollector->seekDocCountTrigger(seekDocCount);
    outCollector->strongJoinFilterCountTrigger(strongJoinFilterCount);
    outCollector->estimatedMatchCountTrigger(estimatedMatchCount);
    outCollector->aggregateCountTrigger(aggregateCount);
    outCollector->reRankCountTrigger(rerankcount);
    if (useTruncateOptimizer) {
        outCollector->increaseUseTruncateOptimizerNum();
    }
}


HA3_LOG_SETUP(turing, Ha3SeekParaMergeOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SeekParaMergeOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekParaMergeOp);

END_HA3_NAMESPACE(turing);
