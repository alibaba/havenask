#pragma once
#include <memory>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/PoolVector.h>
#include <tensorflow/core/framework/op_kernel.h>
#include "ha3/util/Log.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include <ha3/turing/variant/SeekResourceVariant.h>
#include <ha3/turing/variant/InnerResultVariant.h>
#include "ha3/proxy/MatchDocDeduper.h"
#include "ha3/common/ha3_op_common.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearcherProcessorResource.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/HitCollectorManager.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/monitor/SessionMetricsCollector.h"

BEGIN_HA3_NAMESPACE(turing);

class Ha3SeekParaMergeOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SeekParaMergeOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("N", &_wayCount));
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    friend class Ha3SeekParaMergeOpTest;
private:
    bool mergeMatchDocs(
            const std::vector<HA3_NS(search)::InnerSearchResult> &innerResults,
            HA3_NS(search)::InnerSearchResult &mergedInnerResult,
            ErrorCode &errorCode);
    ErrorCode doMergeMatchDocs(
            const std::vector<std::shared_ptr<common::MatchDocs>> &inputMatchDocsVec,
            const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);
    HA3_NS(common)::MatchDocs *createMatchDocs(
            const HA3_NS(search)::InnerSearchResult &innerResult);
    int32_t findFirstNoneEmptyResult(
            const std::vector<HA3_NS(search)::InnerSearchResult>& innerResults);
    void mergeAggResults(
            HA3_NS(common)::AggregateResults &outAggregateResults,
            const std::vector<HA3_NS(search)::InnerSearchResult> &innerResults,
            const HA3_NS(common)::AggregateClause *aggClause,
            autil::mem_pool::Pool *pool);
    void mergeTracer(
            const std::vector<HA3_NS(search)::SearchCommonResourcePtr>&comResources,
            HA3_NS(common)::Tracer *tracer);
    void mergeGlobalVariableManagers(
            const std::vector<HA3_NS(search)::SearchCommonResourcePtr>
            &commonResources,
            HA3_NS(search)::SearchCommonResourcePtr &outCommonResource);

    void mergeErrors(
            const std::vector<HA3_NS(search)::SearchCommonResourcePtr>
            &commonResources,
            HA3_NS(search)::SearchCommonResourcePtr &outCommonResource);
    void mergeMetrics(
            const std::vector<HA3_NS(search)::SearchCommonResourcePtr>
            &commonResources,
            kmonitor::MetricsReporter *userMetricReporter,
            HA3_NS(search)::SearchCommonResourcePtr &outCommonResource);
    std::vector<HA3_NS(search)::InnerSearchResult> filterErrorResults(
            const std::vector<bool> &errorFlag,
            const std::vector<HA3_NS(search)::InnerSearchResult> &inputResults);
    HA3_NS(common)::ResultPtr constructErrorResult(
            std::vector<HA3_NS(search)::SearchCommonResourcePtr> &commonResources,
            SearcherQueryResource *searcherQueryResource);
    HA3_NS(search)::HitCollectorManager *createHitCollectorManager(
            const HA3_NS(search)::SearchCommonResourcePtr &commonResource,
            const HA3_NS(search)::SearchPartitionResourcePtr &partitionResource,
            const HA3_NS(search)::SearchProcessorResource *procsorResource) const;
    bool doRank(const HA3_NS(search)::SearchCommonResourcePtr &commonResource,
                const HA3_NS(search)::SearchPartitionResourcePtr &partitionResource,
                const HA3_NS(search)::SearchProcessorResource *processorResource,
                const HA3_NS(common)::RequestPtr &request,
                HA3_NS(search)::InnerSearchResult &innerResult);

    void prepareResource(
        tensorflow::OpKernelContext *ctx,
        const HA3_NS(common)::RequestPtr request,
        const std::vector<SeekResourcePtr> &seekResources,
        const HA3_NS(common)::Ha3MatchDocAllocatorPtr &allocatorPtr,
        SearcherQueryResource *searcherQueryResource);

    HA3_NS(search)::SearchCommonResourcePtr createCommonResource(
            const HA3_NS(common)::RequestPtr &request,
            const HA3_NS(common)::Ha3MatchDocAllocatorPtr &matchDocAllocator,
            const HA3_NS(turing)::SearcherQueryResource *searcherQueryResource,
            const HA3_NS(search)::SearchCommonResourcePtr &reservedCommonResource);

    void readSeekResourcesFromInput(tensorflow::OpKernelContext* ctx,
                                    std::vector<SeekResourcePtr> &seekResources);
    void readHasErrorsFromInput(tensorflow::OpKernelContext* ctx,
                                std::vector<bool> &hasErrors);
    void readSeekResultsFromInput(
            tensorflow::OpKernelContext* ctx,
            std::vector<HA3_NS(search)::InnerSearchResult> &seekResults);
private:
    int32_t _wayCount;
private:
    kmonitor::MetricsTags _tags;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
