#include <ha3/turing/ops/Ha3ExtraSummaryOp.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(summary);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(func_expression);
BEGIN_HA3_NAMESPACE(turing);

void Ha3ExtraSummaryOp::Compute(tensorflow::OpKernelContext* ctx){

    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);

    SearcherSessionResource *searcherSessionResource =
        dynamic_cast<SearcherSessionResource *>(sessionResource);
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherSessionResource,
                errors::Unavailable("ha3 searcher session resource unavailable"));
    OP_REQUIRES(ctx, searcherQueryResource,
                errors::Unavailable("ha3 searcher query resource unavailable"));
    HA3_NS(service)::SearcherResourcePtr searcherResource =
        searcherSessionResource->searcherResource;
    OP_REQUIRES(ctx, searcherResource,
                errors::Unavailable("ha3 searcher resource unavailable"));

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    RequestPtr request = ha3RequestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    auto resultsTensor = ctx->input(1).flat<Variant>();
    auto ha3ResultVariant = resultsTensor(0).get<Ha3ResultVariant>();
    ResultPtr inputResult = ha3ResultVariant->getResult();
    OP_REQUIRES(ctx, inputResult, errors::Unavailable("ha3 result unavailable"));

    auto tableInfo = sessionResource->dependencyTableInfoMap.find(_tableName);
    if (tableInfo == sessionResource->dependencyTableInfoMap.end()) {
        OP_REQUIRES(ctx, false, errors::Unavailable("find extra table info failed"));
    }

    auto tracer = searcherQueryResource->getTracerPtr();
    bool needDegrade = !(searcherQueryResource->getDegradeLevel() <= 0.0);
    ResultPtr result = inputResult;
    if (!_schemaConsistent) {
        REQUEST_TRACE_WITH_TRACER(tracer.get(), TRACE2, "skip extra summary search. schema is not consistent");
        ErrorResult errorResult(ERROR_GENERAL, "check extra summary schema consistency failed.");
        result->addErrorResult(errorResult);
    } else if (needDegrade) {
        REQUEST_TRACE_WITH_TRACER(tracer.get(), TRACE2, "skip extra summary search. need degrade.");
        auto collectorPtr = searcherQueryResource->sessionMetricsCollector;
        collectorPtr->setExtraPhase2Degradation();
    } else if (!needExtraSearch(inputResult)) {
        REQUEST_TRACE_WITH_TRACER(tracer.get(), TRACE2, "skip extra summary search. no need extra search");
    } else {
        result = supply(request.get(), inputResult, searcherResource,
                        searcherQueryResource, tableInfo->second);
        auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
        addTraceInfo(metricsCollector, tracer.get());
    }

    result->setTracer(tracer);

    auto pool = searcherQueryResource->getPool();
    Ha3ResultVariant ha3Result(result, pool);
    Tensor* out = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {1}, &out));
    out->scalar<Variant>()() = ha3Result;
}

ResultPtr Ha3ExtraSummaryOp::supply(const Request *request,
                                    const ResultPtr& inputResult,
                                    SearcherResourcePtr resourcePtr,
                                    SearcherQueryResource *searcherQueryResource,
                                    const TableInfoPtr &tableInfo)
{
    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    auto timeoutTerminator = searcherQueryResource->getTimeoutTerminator();
    auto tracer = searcherQueryResource->getTracer();
    auto pool = searcherQueryResource->getPool();
    auto cavaAllocator = searcherQueryResource->getCavaAllocator();
    auto cavaJitModules = searcherQueryResource->getCavaJitModules();

    FunctionInterfaceCreatorPtr funcCreatorPtr = resourcePtr->getFuncCreator();

    SummaryProfileManagerPtr summaryProfileManagerPtr = resourcePtr->getSummaryProfileManager();

    SearchCommonResource commonResource(pool, tableInfo, metricsCollector,
            timeoutTerminator, tracer, funcCreatorPtr.get(), resourcePtr->getCavaPluginManager(),
            request, cavaAllocator, cavaJitModules);
    auto hitSummarySchemaPool = resourcePtr->getHitSummarySchemaPool();
    auto hitSummarySchema = hitSummarySchemaPool->get();

    IndexPartitionWrapperPtr indexPartWrapperPtr = resourcePtr->getIndexPartitionWrapper();
    SummarySearcher searcher(commonResource, indexPartWrapperPtr,
                             summaryProfileManagerPtr, hitSummarySchema);
    searcher.setPartitionReaderSnapshot(searcherQueryResource->getIndexSnapshotPtr());
    ResultPtr result = searcher.extraSearch(request, inputResult, tableInfo->getTableName());
    hitSummarySchemaPool->put(hitSummarySchema);

    return result;
}

void Ha3ExtraSummaryOp::addTraceInfo(SessionMetricsCollector *metricsCollector, Tracer* tracer) {
    if (metricsCollector == NULL || tracer == NULL) {
        return;
    }
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE2, "end extra phase two search.");
    tracer->setTracePosFront(true);
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE1,
                              "Searcher_Phase2FetchSummaryLatencyExtra[%fms], "
                              "Searcher_Phase2ExtractSummaryLatencyExtra[%fms], "
                              "Searcher_TotalFetchSummarySizeExtra[%d], ",
                              metricsCollector->getFetchSummaryLatency(SUMMARY_SEARCH_EXTRA),
                              metricsCollector->getExtractSummaryLatency(SUMMARY_SEARCH_EXTRA),
                              metricsCollector->getTotalFetchSummarySize(SUMMARY_SEARCH_EXTRA));

    tracer->setTracePosFront(false);
}

bool Ha3ExtraSummaryOp::needExtraSearch(ResultPtr inputResult) {
    Hits *hits = inputResult->getHits();
    assert(hits);
    for (size_t i = 0; i < hits->size(); i++) {
        auto hit = hits->getHit(i);
        assert(hit);
        if (!hit->hasSummary()) { return true; }
    }
    return false;
}

HA3_LOG_SETUP(turing, Ha3ExtraSummaryOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ExtraSummaryOp")
                        .Device(DEVICE_CPU),
                        Ha3ExtraSummaryOp);

END_HA3_NAMESPACE(turing);
