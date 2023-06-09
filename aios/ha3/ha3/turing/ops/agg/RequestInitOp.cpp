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
#ifndef AIOS_OPEN_SOURCE
#include <Span.h>
#else
#include <aios/network/opentelemetry/core/Span.h>
#endif
#include <stdint.h>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/TimeUtility.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/common/WorkerParam.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
namespace isearch {
namespace turing {

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
        string ip = autil::EnvUtil::getEnv(suez::turing::WorkerParam::IP);
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
            auto span = querySession->getTraceServerSpan();
            REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "searcher session eagle info:");
            if (span) {
                REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "eagle traceid is: %s",
                        opentelemetry::EagleeyeUtil::getETraceId(span).c_str());
                REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "eagle appid is: %s",
                        opentelemetry::EagleeyeUtil::getEAppId(span).c_str());
                const map<string, string> &userDataMap = opentelemetry::EagleeyeUtil::getEUserDatas(span);
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
            AUTIL_LOG(DEBUG, "query config timeout is 0");
        }
        if (queryTimeout > 0 && currentTime - startTime >= queryTimeout) {
            AUTIL_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                    "rpc timeout [%ld]", startTime, currentTime, queryTimeout);
            return timeoutTerminator;
        }

        if (timeout > 0 && currentTime - startTime >= timeout) {
            AUTIL_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
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
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, RequestInitOp);

REGISTER_KERNEL_BUILDER(Name("RequestInitOp")
                        .Device(DEVICE_CPU),
                        RequestInitOp);

} // namespace turing
} // namespace isearch
