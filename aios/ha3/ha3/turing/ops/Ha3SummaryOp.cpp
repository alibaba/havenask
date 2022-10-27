#include <ha3/turing/ops/Ha3SummaryOp.h>

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

void Ha3SummaryOp::Compute(tensorflow::OpKernelContext* ctx){

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

    ResultPtr result = processPhaseTwoRequest(request.get(), searcherResource,
            searcherQueryResource);

    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    auto tracer = searcherQueryResource->getTracerPtr();
    addTraceInfo(metricsCollector, tracer.get());
    result->setTracer(tracer);

    auto pool = searcherQueryResource->getPool();
    Ha3ResultVariant ha3Result(result, pool);
    Tensor* out = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {1}, &out));
    out->scalar<Variant>()() = ha3Result;
}

ResultPtr Ha3SummaryOp::processPhaseTwoRequest(const Request *request,
        SearcherResourcePtr resourcePtr,
        SearcherQueryResource *searcherQueryResource)
{
    DocIdClause* docIdClause = request->getDocIdClause();
    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    if (!docIdClause) {
        if (metricsCollector != nullptr) {
            metricsCollector->increaseSearchPhase2EmptyQps();
        }
        return ResultPtr(new Result());
    }
    auto timeoutTerminator = searcherQueryResource->getTimeoutTerminator();
    auto tracer = searcherQueryResource->getTracer();
    auto pool = searcherQueryResource->getPool();
    auto cavaJitModules = searcherQueryResource->getCavaJitModules();

    IndexPartitionWrapperPtr indexPartWrapperPtr = resourcePtr->getIndexPartitionWrapper();
    TableInfoPtr tableInfoPtr = resourcePtr->getTableInfo();
    FunctionInterfaceCreatorPtr funcCreatorPtr = resourcePtr->getFuncCreator();
    HitSummarySchemaPoolPtr hitSummarySchemaPool = resourcePtr->getHitSummarySchemaPool();
    HitSummarySchemaPtr hitSummarySchemaPtr = hitSummarySchemaPool->get();
    SummaryProfileManagerPtr summaryProfileManagerPtr = resourcePtr->getSummaryProfileManager();
    FullIndexVersion fullIndexVersion = resourcePtr->getFullIndexVersion();
    SearchCommonResource commonResource(pool, tableInfoPtr, metricsCollector,
            timeoutTerminator, tracer, funcCreatorPtr.get(), resourcePtr->getCavaPluginManager(), request,
            searcherQueryResource->getCavaAllocator(), cavaJitModules);
    SummarySearcher searcher(commonResource, indexPartWrapperPtr,
                             summaryProfileManagerPtr, hitSummarySchemaPtr);
    searcher.setIndexPartitionReaderWrapper(searcherQueryResource->indexPartitionReaderWrapper);
    searcher.setPartitionReaderSnapshot(searcherQueryResource->getIndexSnapshotPtr());
    bool oldAllowLackOfSummary = request->getConfigClause()->allowLackOfSummary();
    if (_forceAllowLackOfSummary) {
        request->getConfigClause()->setAllowLackOfSummary(true);
    }
    ResultPtr result = searcher.search(request, resourcePtr->getHashIdRange(), fullIndexVersion);
    hitSummarySchemaPool->put(hitSummarySchemaPtr); // reuse hit schema
    request->getConfigClause()->setAllowLackOfSummary(oldAllowLackOfSummary);
    Hits *hits = result->getHits();
    if (hits && metricsCollector != nullptr) {
        metricsCollector->returnCountTrigger(hits->size());
        if (hits->size() == 0) {
            metricsCollector->increaseSearchPhase2EmptyQps();
        }
    }
    return result;
}

void Ha3SummaryOp::addTraceInfo(SessionMetricsCollector *metricsCollector, Tracer* tracer) {
    if (metricsCollector == NULL || tracer == NULL) {
        return;
    }
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE2, "end phase two search.");
    tracer->setTracePosFront(true);
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE1,
                              "Searcher_Phase2RequestPoolWaitTime[%fms], "
                              "Searcher_Phase2Latency[%fms], "
                              "Searcher_Phase2FetchSummaryLatency[%fms], "
                              "Searcher_Phase2ExtractSummaryLatency[%fms], "
                              "Searcher_TotalFetchSummarySize[%d], "
                              "Searcher_Phase2ResultCount[%d]",
                              metricsCollector->getPoolWaitLatency(),
                              metricsCollector->getProcessLatency(),
                              metricsCollector->getFetchSummaryLatency(SUMMARY_SEARCH_NORMAL),
                              metricsCollector->getExtractSummaryLatency(SUMMARY_SEARCH_NORMAL),
                              metricsCollector->getTotalFetchSummarySize(SUMMARY_SEARCH_NORMAL),
                              metricsCollector->getReturnCount());
    tracer->setTracePosFront(false);
}

HA3_LOG_SETUP(turing, Ha3SummaryOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SummaryOp")
                        .Device(DEVICE_CPU),
                        Ha3SummaryOp);

END_HA3_NAMESPACE(turing);
