#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/util/Log.h>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/common/Tracer.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);

class RequestInitOp : public tensorflow::OpKernel
{
public:
    explicit RequestInitOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();

        auto pool = queryResource->getPool();
        ha3RequestVariant->construct(pool);

        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        ConfigClause *configClause = request->getConfigClause();
        OP_REQUIRES(ctx, configClause, errors::Unavailable("config clause unavailable"));

        string partitionStr = sessionResource->serviceInfo.toString();
        string ip = suez::turing::WorkerParam::getEnv(suez::turing::WorkerParam::IP);
        TracerPtr tracer(configClause->createRequestTracer(partitionStr, ip));
        queryResource->setTracer(tracer);
        REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "actual request is \n %s", request->toString().c_str());
        REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "timeout check in request init op");
        addEagleTraceInfo(queryResource->getGigQuerySession(), tracer.get());
        int64_t currentTime = autil::TimeUtility::currentTime();
        TimeoutTerminatorPtr timeoutTerminator = createTimeoutTerminator(request,
                queryResource->startTime, currentTime, queryResource->getTimeout(0));
        OP_REQUIRES(ctx, timeoutTerminator,
                    errors::Unavailable("timeout, current in query_init_op."));
        queryResource->setTimeoutTerminator(timeoutTerminator);

        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<Variant>()() = *ha3RequestVariant;
    }
private:
    void addEagleTraceInfo(multi_call::QuerySession *querySession, Tracer *tracer) {
        if (querySession) {
            auto rpcContext = querySession->getRpcContext();
            REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "searcher session eagle info:");
            if (rpcContext != nullptr) {
                REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "eagle traceid is: %s",
                        rpcContext->getTraceId().c_str());
                REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "eagle appid is: %s",
                        rpcContext->getAppId().c_str());
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

    TimeoutTerminatorPtr createTimeoutTerminator(const RequestPtr &request,
            int64_t startTime, int64_t currentTime, int32_t timeout)
    {
        TimeoutTerminatorPtr timeoutTerminator;
        const ConfigClause *configClause = request->getConfigClause();
        int64_t queryTimeout = configClause->getRpcTimeOut() * int64_t(1000); //ms->us
        if (queryTimeout == 0) {
            HA3_LOG(DEBUG, "query config timeout is 0");
        }
        if (queryTimeout > 0 && currentTime - startTime >= queryTimeout) {
            HA3_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                    "rpc timeout [%ld]", startTime, currentTime, queryTimeout);
            return timeoutTerminator;
        }

        if (timeout > 0 && currentTime - startTime >= timeout) {
            HA3_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                    "rpc timeout [%d]", startTime, currentTime, timeout);
            return timeoutTerminator;
        }

        int64_t actualTimeOut = 0;
        if (queryTimeout >0 && timeout > 0) {
            actualTimeOut = queryTimeout < timeout ? queryTimeout : timeout;
        } else if (queryTimeout > 0) {
            actualTimeOut = queryTimeout;
        } else {
            actualTimeOut = timeout;
        }
        timeoutTerminator.reset(new TimeoutTerminator(actualTimeOut, startTime));
        return timeoutTerminator;
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, RequestInitOp);

REGISTER_KERNEL_BUILDER(Name("RequestInitOp")
                        .Device(DEVICE_CPU),
                        RequestInitOp);

END_HA3_NAMESPACE(turing);
