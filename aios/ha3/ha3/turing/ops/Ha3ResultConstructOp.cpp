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
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/PoolVector.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/provider/common.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/AggregateResult.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/DataProvider.h"
#include "ha3/common/GlobalVariableManager.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/AggregateResultsVariant.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::rank;
using namespace isearch::monitor;
using namespace isearch::service;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

class Ha3ResultConstructOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultConstructOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
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
        auto pool = searcherQueryResource->getPool();
        auto metricsCollector = searcherQueryResource->sessionMetricsCollector;

        uint32_t ip = searcherSessionResource->getIp();
        IndexPartitionReaderWrapperPtr idxPartReaderWrapperPtr
            = searcherQueryResource->indexPartitionReaderWrapper;

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        auto matchdocsTensor = ctx->input(1).scalar<Variant>()();

        auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant,
                    errors::Unavailable("xxxxx ha3 searcher resource unavailable"));

        matchdoc::MatchDocAllocatorPtr allocator = matchDocsVariant->getAllocator();
        Ha3MatchDocAllocatorPtr ha3Allocator =
            dynamic_pointer_cast<Ha3MatchDocAllocator>(allocator);
        OP_REQUIRES(ctx, ha3Allocator, errors::Unavailable("dynamic_pointer_cast allocator failed."));
        InnerSearchResult innerResult(pool);
        innerResult.matchDocAllocatorPtr = ha3Allocator;

        vector<matchdoc::MatchDoc> matchDocVec;
        matchDocsVariant->stealMatchDocs(matchDocVec);
        innerResult.matchDocVec.insert(innerResult.matchDocVec.begin(),
                matchDocVec.begin(), matchDocVec.end());

        uint32_t totalMatchDocs = ctx->input(2).scalar<uint32_t>()();
        uint32_t actualMatchDocs = ctx->input(3).scalar<uint32_t>()();

        innerResult.totalMatchDocs = totalMatchDocs;
        innerResult.actualMatchDocs = actualMatchDocs;

        auto aggResultsTensor = ctx->input(4).scalar<Variant>()();
        auto aggResultsVariant = aggResultsTensor.get<AggregateResultsVariant>();

        innerResult.aggResultsPtr = aggResultsVariant->getAggResults();
        TracerPtr tracer = searcherQueryResource->getTracerPtr();
        auto commonResource = searcherQueryResource->commonResource;
        common::ResultPtr resultPtr(constructResult(innerResult, metricsCollector, tracer, commonResource));
        if (commonResource) {
            resultPtr->addErrorResult(commonResource->errorResult);
        }
        MatchDocs *matchDocs = resultPtr->getMatchDocs();
        auto range = searcherQueryResource->partRange;
        if (matchDocs) {
            auto phaseOneInfoMask = request->getConfigClause()->getPhaseOneBasicInfoMask();
            versionid_t versionId = idxPartReaderWrapperPtr->getCurrentVersion();
            matchDocs->setGlobalIdInfo(range.first, versionId,
                    searcherSessionResource->mainTableFullVersion, ip, phaseOneInfoMask);
        }

        string clusterName = searcherSessionResource->searcherResource->getClusterName();
        resultPtr->addCoveredRange(clusterName, range.first, range.second);
        Ha3ResultVariant ha3Result(resultPtr, pool);
        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<Variant>()() = ha3Result;
    }

private:
    common::ResultPtr constructResult(InnerSearchResult& innerResult,
            SessionMetricsCollector *sessionMetricsCollector, TracerPtr &tracer, SearchCommonResourcePtr &resource)
    {
        sessionMetricsCollector->estimatedMatchCountTrigger(innerResult.totalMatchDocs);
        common::MatchDocs *matchDocs = createMatchDocs(innerResult);
        common::ResultPtr result(new common::Result(matchDocs));
        result->fillAggregateResults(innerResult.aggResultsPtr);
        result->setUseTruncateOptimizer(sessionMetricsCollector->useTruncateOptimizer());
        fillGlobalInformations(result, resource);
        if (matchDocs) {
            sessionMetricsCollector->returnCountTrigger(matchDocs->size());
        }
        addTraceInfo(sessionMetricsCollector, tracer.get());
        result->setTracer(tracer);
        return result;
    }

    void fillGlobalInformations(ResultPtr &result,
                                SearchCommonResourcePtr &resource)
    {
        if (!resource) return;
        const common::AttributeItemMapPtr &v =
            resource->dataProvider.getNeedSerializeGlobalVariables();
        if (v && !v->empty()) {
            result->addGlobalVariableMap(v);
        }
        for (auto &manager : resource->paraGlobalVariableManagers) {
            const common::AttributeItemMapPtr &globalVarMap =
                manager->getNeedSerializeGlobalVariables();
            if (globalVarMap && !globalVarMap->empty()) {
                result->addGlobalVariableMap(globalVarMap);
            }
        }
    }

    common::MatchDocs *createMatchDocs(InnerSearchResult &innerResult) const {
        common::MatchDocs *matchDocs = new common::MatchDocs();
        matchDocs->setTotalMatchDocs(innerResult.totalMatchDocs);
        matchDocs->setActualMatchDocs(innerResult.actualMatchDocs);
        if (innerResult.matchDocAllocatorPtr) {
            innerResult.matchDocAllocatorPtr->dropField(SIMPLE_MATCH_DATA_REF);
            innerResult.matchDocAllocatorPtr->dropField(MATCH_DATA_REF);
        }
        matchDocs->setMatchDocAllocator(innerResult.matchDocAllocatorPtr);
        matchDocs->resize(innerResult.matchDocVec.size());
        AUTIL_LOG(TRACE3, "matchDocVect.size()=[%zd]", innerResult.matchDocVec.size());
        for (uint32_t i = 0; i < innerResult.matchDocVec.size(); i++) {
            matchDocs->insertMatchDoc(i, innerResult.matchDocVec[i]);
        }
        return matchDocs;
    }
    void addTraceInfo(SessionMetricsCollector *metricsCollector, Tracer* tracer) {
        if (metricsCollector == NULL ||tracer == NULL) {
            return;
        }
        REQUEST_TRACE_WITH_TRACER(tracer, TRACE2, "end phase one search.");
        tracer->setTracePosFront(true);
        REQUEST_TRACE_WITH_TRACER(tracer,TRACE1, "Searcher_Phase1RequestPoolWaitTime[%fms], "
                      "Searcher_Phase1Latency[%fms], "
                  "Searcher_BeforeSearchLatency[%fms], "
                  "Searcher_Phase1RankLatency[%fms], "
                  "Searcher_Phase1ReRankLatency[%fms], "
                  "Searcher_AfterSearchLatency[%fms], "
                  "Searcher_Phase1SeekCount[%d], "
                  "Searcher_Phase1RerankCount[%d], "
                  "Searcher_Phase1ReturnCount[%d], "
                  "Searcher_Phase1TotalMatchDocCount[%d], "
                  "Searcher_Phase1AggregateCount[%d], "
                  "Searcher_useTruncateOptimizer[%s], "
                  "Searcher_cacheHit[%s]",
                  metricsCollector->getPoolWaitLatency(),
                  metricsCollector->getProcessLatency(),
                  metricsCollector->getBeforeSearchLatency(),
                  metricsCollector->getRankLatency(),
                  metricsCollector->getReRankLatency(),
                  metricsCollector->getAfterSearchLatency(),
                  metricsCollector->getSeekCount(),
                  metricsCollector->getReRankCount(),
                  metricsCollector->getReturnCount(),
                  metricsCollector->getTotalMatchCount(),
                  metricsCollector->getAggregateCount(),
                  (metricsCollector->useTruncateOptimizer() ? "true" : "false"),
                  (metricsCollector->isCacheHit() ? "true" : "false"));
        tracer->setTracePosFront(false);
    }

private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, Ha3ResultConstructOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultConstructOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultConstructOp);

} // namespace turing
} // namespace isearch
