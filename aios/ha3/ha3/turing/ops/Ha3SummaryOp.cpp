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
#include "ha3/turing/ops/Ha3SummaryOp.h"

#include <stddef.h>
#include <iostream>
#include <memory>
#include <memory>

#include "turing_ops_util/util/OpUtil.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/Hits.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SummarySearcher.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/summary/SummaryProfileManager.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
class DocIdClause;
}  // namespace common
}  // namespace isearch

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::search;
using namespace isearch::config;
using namespace isearch::monitor;
using namespace isearch::summary;
using namespace isearch::service;

namespace isearch {
namespace turing {

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
    isearch::service::SearcherResourcePtr searcherResource =
        searcherSessionResource->searcherResource;
    OP_REQUIRES(ctx, searcherResource,
                errors::Unavailable("ha3 searcher resource unavailable"));

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    RequestPtr request = ha3RequestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    ResultPtr result = processPhaseTwoRequest(request.get(), searcherResource,
            searcherSessionResource, searcherQueryResource);

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
        SearcherSessionResource *searcherSessionResource,
        SearcherQueryResource *searcherQueryResource)
{
    DocIdClause* docIdClause = request->getDocIdClause();
    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    if (!docIdClause) {
        if (metricsCollector != nullptr) {
            metricsCollector->increaseSearchPhase2EmptyQps();
        }
        return ResultPtr(new common::Result());
    }
    auto timeoutTerminator = searcherQueryResource->getTimeoutTerminator();
    auto tracer = searcherQueryResource->getTracer();
    auto pool = searcherQueryResource->getPool();
    auto cavaJitModules = searcherQueryResource->getCavaJitModulesWrapper();

    TableInfoPtr tableInfoPtr = resourcePtr->getTableInfo();
    FunctionInterfaceCreatorPtr funcCreatorPtr = resourcePtr->getFuncCreator();
    HitSummarySchemaPoolPtr hitSummarySchemaPool = resourcePtr->getHitSummarySchemaPool();
    HitSummarySchemaPtr hitSummarySchemaPtr = hitSummarySchemaPool->get();
    SummaryProfileManagerPtr summaryProfileManagerPtr = resourcePtr->getSummaryProfileManager();
    FullIndexVersion fullIndexVersion = searcherSessionResource->mainTableFullVersion;
    SearchCommonResource commonResource(pool, tableInfoPtr, metricsCollector,
            timeoutTerminator, tracer, funcCreatorPtr.get(),
            resourcePtr->getCavaPluginManager().get(), request,
            searcherQueryResource->getCavaAllocator(), cavaJitModules);
    SummarySearcher searcher(commonResource, searcherQueryResource->indexPartitionReaderWrapper,
                             searcherQueryResource->getIndexSnapshot(),
                             summaryProfileManagerPtr, hitSummarySchemaPtr);
    bool oldAllowLackOfSummary = request->getConfigClause()->allowLackOfSummary();
    if (_forceAllowLackOfSummary) {
        request->getConfigClause()->setAllowLackOfSummary(true);
    }
    proto::Range range;
    range.set_from(searcherQueryResource->partRange.first);
    range.set_to(searcherQueryResource->partRange.second);
    ResultPtr result = searcher.search(request, range, fullIndexVersion);
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

AUTIL_LOG_SETUP(ha3, Ha3SummaryOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SummaryOp")
                        .Device(DEVICE_CPU),
                        Ha3SummaryOp);

} // namespace turing
} // namespace isearch
