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
#pragma once
#include <memory>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolVector.h"
#include "tensorflow/core/framework/op_kernel.h"

#include "ha3/common/AggregateResult.h"
#include "ha3/common/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proxy/MatchDocDeduper.h"
#include "ha3/search/HitCollectorManager.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearcherProcessorResource.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "ha3/turing/variant/InnerResultVariant.h"
#include "ha3/turing/variant/SeekResourceVariant.h"
#include "autil/Log.h"

namespace isearch {
namespace turing {

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
            const std::vector<isearch::search::InnerSearchResult> &innerResults,
            isearch::search::InnerSearchResult &mergedInnerResult,
            ErrorCode &errorCode);
    ErrorCode doMergeMatchDocs(
            const std::vector<std::shared_ptr<common::MatchDocs>> &inputMatchDocsVec,
            const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);
    isearch::common::MatchDocs *createMatchDocs(
            const isearch::search::InnerSearchResult &innerResult);
    int32_t findFirstNoneEmptyResult(
            const std::vector<isearch::search::InnerSearchResult>& innerResults);
    void mergeAggResults(
            isearch::common::AggregateResults &outAggregateResults,
            const std::vector<isearch::search::InnerSearchResult> &innerResults,
            const isearch::common::AggregateClause *aggClause,
            autil::mem_pool::Pool *pool);
    void mergeTracer(
            const std::vector<isearch::search::SearchCommonResourcePtr>&comResources,
            isearch::common::Tracer *tracer);
    void mergeGlobalVariableManagers(
            const std::vector<isearch::search::SearchCommonResourcePtr>
            &commonResources,
            isearch::search::SearchCommonResourcePtr &outCommonResource);

    void mergeErrors(
            const std::vector<isearch::search::SearchCommonResourcePtr>
            &commonResources,
            isearch::search::SearchCommonResourcePtr &outCommonResource);
    void mergeMetrics(
            const std::vector<isearch::search::SearchCommonResourcePtr>
            &commonResources,
            kmonitor::MetricsReporter *userMetricReporter,
            isearch::search::SearchCommonResourcePtr &outCommonResource);
    std::vector<isearch::search::InnerSearchResult> filterErrorResults(
            const std::vector<bool> &errorFlag,
            const std::vector<isearch::search::InnerSearchResult> &inputResults);
    isearch::common::ResultPtr constructErrorResult(
            std::vector<isearch::search::SearchCommonResourcePtr> &commonResources,
            SearcherQueryResource *searcherQueryResource);
    isearch::search::HitCollectorManager *createHitCollectorManager(
            const isearch::search::SearchCommonResourcePtr &commonResource,
            const isearch::search::SearchPartitionResourcePtr &partitionResource,
            const isearch::search::SearchProcessorResource *procsorResource) const;
    bool doRank(const isearch::search::SearchCommonResourcePtr &commonResource,
                const isearch::search::SearchPartitionResourcePtr &partitionResource,
                const isearch::search::SearchProcessorResource *processorResource,
                const isearch::common::RequestPtr &request,
                isearch::search::InnerSearchResult &innerResult);

    void prepareResource(
        tensorflow::OpKernelContext *ctx,
        const isearch::common::RequestPtr request,
        const std::vector<SeekResourcePtr> &seekResources,
        const isearch::common::Ha3MatchDocAllocatorPtr &allocatorPtr,
        SearcherQueryResource *searcherQueryResource);

    isearch::search::SearchCommonResourcePtr createCommonResource(
            const isearch::common::RequestPtr &request,
            const isearch::common::Ha3MatchDocAllocatorPtr &matchDocAllocator,
            const isearch::turing::SearcherQueryResource *searcherQueryResource,
            const isearch::search::SearchCommonResourcePtr &reservedCommonResource);

    void readSeekResourcesFromInput(tensorflow::OpKernelContext* ctx,
                                    std::vector<SeekResourcePtr> &seekResources);
    void readHasErrorsFromInput(tensorflow::OpKernelContext* ctx,
                                std::vector<bool> &hasErrors);
    void readSeekResultsFromInput(
            tensorflow::OpKernelContext* ctx,
            std::vector<isearch::search::InnerSearchResult> &seekResults);
private:
    int32_t _wayCount;
private:
    kmonitor::MetricsTags _tags;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
