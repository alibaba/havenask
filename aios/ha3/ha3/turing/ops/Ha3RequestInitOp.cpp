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
#include "ha3/turing/ops/Ha3RequestInitOp.h"

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/StringUtil.h"
#include "turing_ops_util/util/OpUtil.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "autil/SharedObjectMap.h"
#include "suez/turing/metrics/BasicBizMetrics.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/ProviderBase.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h"

using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::service;
using namespace isearch::search;
using namespace isearch::common;
using namespace isearch::config;
using namespace isearch::monitor;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, Ha3RequestInitOp);

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
    isearch::service::SearcherResourcePtr searcherResource =
        searcherSessionResource->searcherResource;
    OP_REQUIRES(ctx, searcherResource,
                errors::Unavailable("ha3 searcher resource unavailable"));
    auto metricsCollector =  searcherQueryResource->sessionMetricsCollector;
    OP_REQUIRES(ctx, metricsCollector,
                errors::Unavailable("ha3 session metrics collector cast failed."));

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    auto pool = searcherQueryResource->getPool();
    ha3RequestVariant->construct(pool);
    RequestPtr request = ha3RequestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    ConfigClause *configClause = request->getConfigClause();
    OP_REQUIRES(ctx, configClause, errors::Unavailable("config clause unavailable"));
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    if (cacheClause) {
        cacheClause->setPartRange(queryResource->partRange);
    }
    const auto &range = searcherQueryResource->partRange;
    string partStr = "SEARCHER_" + searcherSessionResource->searcherResource->getClusterName()
                     + "_0_0_" + StringUtil::toString(range.first) + "_"
                     + StringUtil::toString(range.second);
    TracerPtr tracer(configClause->createRequestTracer(partStr, searcherSessionResource->ip));
    searcherQueryResource->setTracer(tracer);
    searcherQueryResource->rankTraceLevel =
        isearch::search::ProviderBase::convertRankTraceLevel(request.get());
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "actual request is \n %s", request->toString().c_str());
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "timeout check in request init op");
    addEagleTraceInfo(queryResource->getGigQuerySession(), tracer.get());
    auto indexSnapshot = searcherQueryResource->getIndexSnapshot();
    OP_REQUIRES(ctx, indexSnapshot, errors::Unavailable("index snapshot unavailable"));
    searcherQueryResource->indexPartitionReaderWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(indexSnapshot,
                searcherSessionResource->mainTableSchemaName);
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
        auto span = querySession->getTraceServerSpan();
        REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "searcher session eagle info:");
        if (span) {
            REQUEST_TRACE_WITH_TRACER(tracer, TRACE3, "eagle traceid is: %s",
                    opentelemetry::EagleeyeUtil::getETraceId(span).c_str());
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

TimeoutTerminatorPtr Ha3RequestInitOp::createTimeoutTerminator(const RequestPtr &request,
        int64_t startTime, int64_t currentTime, bool isSeekTimeout)
{
    TimeoutTerminatorPtr timeoutTerminator;
    const ConfigClause *configClause = request->getConfigClause();
    int64_t timeout = configClause->getRpcTimeOut() * int64_t(MS_TO_US_FACTOR); //ms->us
    if (timeout == 0) {
        AUTIL_LOG(DEBUG, "timeout is 0");
    }
    if (timeout > 0 && currentTime - startTime >= timeout) {
        AUTIL_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
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

} // namespace turing
} // namespace isearch
