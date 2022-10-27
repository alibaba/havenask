#include <ha3/turing/ops/Ha3RequestInitOp.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, Ha3RequestInitOp);

void Ha3RequestInitOp::Compute(tensorflow::OpKernelContext* ctx) {
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
    searcherQueryResource->sessionMetricsCollector =
        dynamic_cast<SessionMetricsCollector*>(searcherQueryResource->getBasicMetricsCollector());

    auto metricsCollector =  searcherQueryResource->sessionMetricsCollector;
    OP_REQUIRES(ctx, metricsCollector,
                errors::Unavailable("ha3 session metrics collector cast failed."));

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    auto pool = searcherQueryResource->getPool();
    ha3RequestVariant->construct(pool);
    RequestPtr request = ha3RequestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
    //request->setPool(searcherQueryResource->getPool());

    ConfigClause *configClause = request->getConfigClause();
    OP_REQUIRES(ctx, configClause, errors::Unavailable("config clause unavailable"));
    proto::PartitionID partitionId = searcherSessionResource->searcherResource->getPartitionId();
    string partitionStr = proto::ProtoClassUtil::partitionIdToString(partitionId);
    TracerPtr tracer;
    tracer.reset(configClause->createRequestTracer(
                    proto::ProtoClassUtil::simplifyPartitionIdStr(partitionStr),
                    searcherSessionResource->ip));
    searcherQueryResource->setTracer(tracer);
    searcherQueryResource->rankTraceLevel =
        isearch::search::ProviderBase::convertRankTraceLevel(request.get());
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "actual request is \n %s", request->toString().c_str());
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "timeout check in request init op");
    addEagleTraceInfo(queryResource->getGigQuerySession(), tracer.get());
    auto indexSnapshot = searcherQueryResource->getIndexSnapshotPtr();
    OP_REQUIRES(ctx, indexSnapshot, errors::Unavailable("index snapshot unavailable"));

    searcherQueryResource->indexPartitionReaderWrapper =
        searcherSessionResource->searcherResource->createIndexPartitionReaderWrapper(indexSnapshot);

    metricsCollector->setTracer(tracer.get());

    int64_t currentTime = autil::TimeUtility::currentTime();
    uint32_t phaseNumber = configClause->getPhaseNumber();

    if (phaseNumber == SEARCH_PHASE_ONE) {
        TimeoutTerminatorPtr timeoutTerminator = createTimeoutTerminator(request,
                searcherQueryResource->startTime, currentTime, false);
        OP_REQUIRES(ctx, timeoutTerminator,
                    errors::Unavailable("timeout, current in query_init_op."));
        searcherQueryResource->setTimeoutTerminator(timeoutTerminator);
        TimeoutTerminatorPtr seekTimeoutTerminator = createTimeoutTerminator(request,
                searcherQueryResource->startTime, currentTime, true);
        OP_REQUIRES(ctx, seekTimeoutTerminator,
                    errors::Unavailable("timeout, current in query_init_op."));
        searcherQueryResource->setSeekTimeoutTerminator(seekTimeoutTerminator);
    } else {
        TimeoutTerminatorPtr timeoutTerminator = createTimeoutTerminator(request,
                searcherQueryResource->startTime, currentTime, true);
        OP_REQUIRES(ctx, timeoutTerminator,
                    errors::Unavailable("timeout, current in query_init_op."));
        searcherQueryResource->setTimeoutTerminator(timeoutTerminator);
    }

    if (request->getVirtualAttributeClause()) {
        auto &virtualAttributes = request->getVirtualAttributeClause()->getVirtualAttributes();
        searcherQueryResource->sharedObjectMap.setWithoutDelete(VIRTUAL_ATTRIBUTES,
                (void *)(&virtualAttributes));
    }

    Tensor* out = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
    out->scalar<Variant>()() = *ha3RequestVariant;
}

void Ha3RequestInitOp::addEagleTraceInfo(multi_call::QuerySession *querySession, Tracer *tracer) {
    if (querySession) {
        auto rpcContext = querySession->getRpcContext();
        REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "searcher session eagle info:");
        if (rpcContext != nullptr) {
            REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "eagle traceid is: %s",
                    rpcContext->getTraceId().c_str());
            const map<string, string> &userDataMap = rpcContext->getUserDataMap();
            string userData;
            for (auto iter = userDataMap.begin(); iter != userDataMap.end(); iter++) {
                userData += iter->first + "=" + iter->second + ";";
            }
            REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "eagle userdata is: %s",
                    userData.c_str());
        }
    }
}

TimeoutTerminatorPtr Ha3RequestInitOp::createTimeoutTerminator(const RequestPtr &request,
        int64_t startTime, int64_t currentTime, bool isSeekTimeout)
{
    TimeoutTerminatorPtr timeoutTerminator;
    const ConfigClause *configClause = request->getConfigClause();
    int64_t timeout = configClause->getRpcTimeOut() * int64_t(MS_TO_US_FACTOR); //ms->us
    if (timeout == 0) {
        HA3_LOG(DEBUG, "timeout is 0");
    }
    if (timeout > 0 && currentTime - startTime >= timeout) {
        HA3_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                "rpc timeout [%ld]", startTime, currentTime, timeout);
        return timeoutTerminator;
    }
    if (isSeekTimeout) {
        int64_t seekTimeOut = configClause->getSeekTimeOut() * int64_t(MS_TO_US_FACTOR); //ms->us
        int64_t actualTimeOut = seekTimeOut ? seekTimeOut :
                                int64_t(timeout * SEEK_TIMEOUT_PERCENTAGE);
        timeoutTerminator.reset(new TimeoutTerminator(actualTimeOut, startTime));
    }
    else {
        timeoutTerminator.reset(new TimeoutTerminator(timeout, startTime));
    }
    return timeoutTerminator;
}

REGISTER_KERNEL_BUILDER(Name("Ha3RequestInitOp")
                        .Device(DEVICE_CPU),
                        Ha3RequestInitOp);

END_HA3_NAMESPACE(turing);
