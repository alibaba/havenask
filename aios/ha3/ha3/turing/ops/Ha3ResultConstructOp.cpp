#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/variant/AggregateResultsVariant.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/TimeoutTerminator.h>
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(func_expression);

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
        HA3_NS(service)::SearcherResourcePtr searcherResource =
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
        if (matchDocs) {
            auto phaseOneInfoMask = request->getConfigClause()->getPhaseOneBasicInfoMask();
            versionid_t versionId = idxPartReaderWrapperPtr->getCurrentVersion();
            matchDocs->setGlobalIdInfo(searcherResource->getHashIdRange().from(),
                    versionId, searcherResource->getFullIndexVersion(), ip, phaseOneInfoMask);
        }

        const proto::PartitionID &partitionId = searcherResource->getPartitionId();
        resultPtr->addCoveredRange(partitionId.clustername(),
                partitionId.range().from(),
                partitionId.range().to());

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
            innerResult.matchDocAllocatorPtr->dropField(common::SIMPLE_MATCH_DATA_REF);
            innerResult.matchDocAllocatorPtr->dropField(common::MATCH_DATA_REF);
        }
        matchDocs->setMatchDocAllocator(innerResult.matchDocAllocatorPtr);
        matchDocs->resize(innerResult.matchDocVec.size());
        HA3_LOG(TRACE3, "matchDocVect.size()=[%zd]", innerResult.matchDocVec.size());
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
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3ResultConstructOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultConstructOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultConstructOp);

END_HA3_NAMESPACE(turing);
