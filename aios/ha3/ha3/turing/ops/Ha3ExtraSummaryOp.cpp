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
#include "ha3/turing/ops/Ha3ExtraSummaryOp.h"

#include <assert.h>
#include <stddef.h>
#include <iostream>
#include <memory>
#include <type_traits>

#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Hit.h"
#include "ha3/common/Hits.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
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

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
using namespace isearch::monitor;
using namespace isearch::summary;
using namespace isearch::service;
namespace isearch {
namespace turing {

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
    isearch::service::SearcherResourcePtr searcherResource =
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
    auto cavaJitModules = searcherQueryResource->getCavaJitModulesWrapper();

    FunctionInterfaceCreatorPtr funcCreatorPtr = resourcePtr->getFuncCreator();

    SummaryProfileManagerPtr summaryProfileManagerPtr = resourcePtr->getSummaryProfileManager();

    SearchCommonResource commonResource(pool, tableInfo, metricsCollector,
            timeoutTerminator, tracer, funcCreatorPtr.get(),
            resourcePtr->getCavaPluginManager().get(),
            request, cavaAllocator, cavaJitModules);
    auto hitSummarySchemaPool = resourcePtr->getHitSummarySchemaPool();
    auto hitSummarySchema = hitSummarySchemaPool->get();

    SummarySearcher searcher(commonResource, searcherQueryResource->indexPartitionReaderWrapper,
                             searcherQueryResource->getIndexSnapshot(),
                             summaryProfileManagerPtr, hitSummarySchema);
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

AUTIL_LOG_SETUP(ha3, Ha3ExtraSummaryOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ExtraSummaryOp")
                        .Device(DEVICE_CPU),
                        Ha3ExtraSummaryOp);

} // namespace turing
} // namespace isearch
