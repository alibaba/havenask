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
#include "ha3/turing/ops/Ha3SeekOp.h"

#include <stdint.h>
#include <iostream>
#include <memory>
#include <memory>
#include <vector>

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
#include "suez/turing/expression/plugin/SorterManager.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDocSearcher.h"
#include "ha3/search/OptimizerChainManager.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/AggregateResultsVariant.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class RankProfile;
}  // namespace rank
}  // namespace isearch

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::rank;
using namespace isearch::service;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

AUTIL_LOG_SETUP(ha3, Ha3SeekOp);

void Ha3SeekOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    SearcherSessionResource *searcherSessionResource = dynamic_cast<SearcherSessionResource *>(sessionResource);
    SearcherQueryResource *searcherQueryResource =  dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherSessionResource,
                      errors::Unavailable("ha3 searcher session resource unavailable"));
    OP_REQUIRES(ctx, searcherQueryResource,
                      errors::Unavailable("ha3 searcher query resource unavailable"));
    isearch::service::SearcherResource *searcherResource =
        searcherSessionResource->searcherResource.get();
    OP_REQUIRES(ctx, searcherResource,
                      errors::Unavailable("ha3 searcher resource unavailable"));
    auto pool = searcherQueryResource->getPool();
    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    Request *request = ha3RequestVariant->getRequest().get();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    const rank::RankProfile *rankProfile = nullptr;
    if (!rankProfileMgr->getRankProfile(request,
                    rankProfile, searcherQueryResource->commonResource->errorResult))
    {
        OP_REQUIRES(ctx, false, errors::Unavailable("get rank profile failed."));
    }
    seek(ctx, request, rankProfile, pool, searcherSessionResource,
         searcherQueryResource,  searcherResource);
}

void Ha3SeekOp::seek(tensorflow::OpKernelContext* ctx,
                     common::Request *request,
                     const rank::RankProfile *rankProfile,
                     autil::mem_pool::Pool *pool,
                     SearcherSessionResource *searcherSessionResource,
                     SearcherQueryResource *searcherQueryResource,
                     service::SearcherResource *searcherResource)
{
    search::InnerSearchResult innerResult(pool);
    bool ret = processRequest(request, rankProfile, searcherQueryResource, searcherResource, innerResult);
    outputResult(ctx, ret, innerResult, request, pool, searcherSessionResource, searcherQueryResource, searcherResource);
}

void Ha3SeekOp::outputResult(tensorflow::OpKernelContext* ctx,
                             bool ret,
                             search::InnerSearchResult &innerResult,
                             common::Request *request,
                             autil::mem_pool::Pool *pool,
                             SearcherSessionResource *searcherSessionResource,
                             SearcherQueryResource *searcherQueryResource,
                             service::SearcherResource *searcherResource)
{
    if (!ret) {
        if (searcherQueryResource) {
            auto commonResource = searcherQueryResource->commonResource.get();
            if (commonResource) {
                common::ResultPtr errorResult = constructErrorResult(request, searcherSessionResource,
                        searcherQueryResource, searcherResource, commonResource);
                Ha3ResultVariant ha3ErrorResultVariant(errorResult, pool);
                tensorflow::Tensor* errorResultTensor = nullptr;
                OP_REQUIRES_OK(ctx, ctx->allocate_output(5, {}, &errorResultTensor));
                errorResultTensor->scalar<tensorflow::Variant>()() = ha3ErrorResultVariant;
                return;
            }
        }
        OP_REQUIRES(ctx, false, tensorflow::errors::Unavailable("construct error result failed."));
    }
    common::Ha3MatchDocAllocatorPtr ha3Allocator = innerResult.matchDocAllocatorPtr;
    matchdoc::MatchDocAllocatorPtr allocator =
        std::dynamic_pointer_cast<matchdoc::MatchDocAllocator>(ha3Allocator);
    OP_REQUIRES(ctx, allocator,
                      tensorflow::errors::Unavailable("dynamic_pointer_cast ha3Allocator failed."));

    suez::turing::MatchDocsVariant matchDocsVariant(allocator, pool);
    std::vector<matchdoc::MatchDoc> matchDocVec;
    matchDocVec.insert(matchDocVec.begin(),
                       innerResult.matchDocVec.begin(), innerResult.matchDocVec.end());
    matchDocsVariant.stealMatchDocs(matchDocVec);

    tensorflow::Tensor* matchDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocs));
    matchDocs->scalar<tensorflow::Variant>()() = matchDocsVariant;

    tensorflow::Tensor* extraDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &extraDocs));
    extraDocs->scalar<uint32_t>()() = innerResult.extraRankCount;

    tensorflow::Tensor* totalDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &totalDocs));
    totalDocs->scalar<uint32_t>()() = innerResult.totalMatchDocs;

    tensorflow::Tensor* actualDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(3, {}, &actualDocs));
    actualDocs->scalar<uint32_t>()() = innerResult.actualMatchDocs;


    AggregateResultsVariant ha3AggResultsVariant(innerResult.aggResultsPtr,  pool);
    tensorflow::Tensor* aggResults = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(4, {}, &aggResults));
    aggResults->scalar<tensorflow::Variant>()() = ha3AggResultsVariant;

}

bool Ha3SeekOp::processRequest(common::Request *request,
                               const rank::RankProfile *rankProfile,
                               SearcherQueryResource *searcherQueryResource,
                               service::SearcherResource *searcherResource,
                               search::InnerSearchResult &innerResult)
{
    auto &commonResource = searcherQueryResource->commonResource;
    auto &partitionResource = searcherQueryResource->partitionResource;
    auto &runtimeResource = searcherQueryResource->runtimeResource;
    if (!commonResource || !partitionResource || !runtimeResource) {
        return false;
    }
    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    auto optimizerChainManager = searcherResource->getOptimizerChainManager().get();
    auto sorterManager = searcherResource->getSorterManager().get();
    auto searcherCache = searcherResource->getSearcherCache().get();
    search::MatchDocSearcher searcher(*commonResource, *partitionResource, *runtimeResource,
            rankProfileMgr, optimizerChainManager, sorterManager,
            searcherResource->getAggSamplerConfigInfo(),
            searcherResource->getClusterConfig(),
            searcherCache, rankProfile,
            search::LayerMetasPtr(), searcherQueryResource->ha3BizMeta);

    search::SearcherCacheInfoPtr searcherCacheInfo = searcherQueryResource->searcherCacheInfo;
    return searcher.seek(request,searcherCacheInfo.get(), innerResult);
}

common::ResultPtr Ha3SeekOp::constructErrorResult(common::Request *request,
        SearcherSessionResource *searcherSessionResource,
        SearcherQueryResource *searcherQueryResource,
        service::SearcherResource *searcherResource,
        search::SearchCommonResource *commonResource)
{
    common::MatchDocs *matchDocs = new common::MatchDocs();
    common::ResultPtr result = common::ResultPtr(new common::Result(matchDocs));
    result->addErrorResult(commonResource->errorResult);
    auto tracer = searcherQueryResource->getTracerPtr();
    result->setTracer(tracer);

    if (request && searcherResource && searcherSessionResource && searcherQueryResource) {
        uint32_t ip = searcherSessionResource->getIp();
        auto idxPartReaderWrapperPtr
            = searcherQueryResource->indexPartitionReaderWrapper;
        auto phaseOneInfoMask = request->getConfigClause()->getPhaseOneBasicInfoMask();
        versionid_t versionId = idxPartReaderWrapperPtr->getCurrentVersion();
        auto range = searcherQueryResource->partRange;
        matchDocs->setGlobalIdInfo(range.first,
                versionId, searcherSessionResource->mainTableFullVersion, ip, phaseOneInfoMask);
        result->addCoveredRange(searcherResource->getClusterName(), range.first, range.second);
    }
    return result;
}

REGISTER_KERNEL_BUILDER(Name("Ha3SeekOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekOp);

} // namespace turing
} // namespace isearch
