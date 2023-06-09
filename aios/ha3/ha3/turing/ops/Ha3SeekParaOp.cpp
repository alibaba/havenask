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
#include "ha3/turing/ops/Ha3SeekParaOp.h"

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <memory>

#include "turing_ops_util/util/OpUtil.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearcher.h"
#include "ha3/search/OptimizerChainManager.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/InnerResultVariant.h"
#include "ha3/turing/variant/SeekResourceVariant.h"
#include "autil/Log.h"

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

void Ha3SeekParaOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    SearcherSessionResource *searcherSessionResource = dynamic_cast<SearcherSessionResource *>(sessionResource);
    OP_REQUIRES(ctx, searcherSessionResource,
                      errors::Unavailable("ha3 searcher session resource unavailable"));
    isearch::service::SearcherResource *searcherResource =
        searcherSessionResource->searcherResource.get();
    OP_REQUIRES(ctx, searcherResource,
                      errors::Unavailable("ha3 searcher resource unavailable"));
    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, ha3RequestVariant,
                      errors::Unavailable("ha3 request variant unavailable"));
    Request *request = ha3RequestVariant->getRequest().get();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
    auto seekResourceVariant = ctx->input(1).scalar<Variant>()().get<SeekResourceVariant>();
    OP_REQUIRES(ctx, seekResourceVariant,
                      errors::Unavailable("seek resource variant unavailable"));
    SeekResource *seekResource = seekResourceVariant->getSeekResource().get();
    OP_REQUIRES(ctx, seekResource, errors::Unavailable("seek resource unavailable"));
    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    const rank::RankProfile *rankProfile = nullptr;
    if (!rankProfileMgr->getRankProfile(request,
                    rankProfile, seekResource->commonResource->errorResult))
    {
        OP_REQUIRES(ctx, false, errors::Unavailable("get rank profile failed."));
    }
    seek(ctx, request, rankProfile, seekResource, searcherResource);
}

void Ha3SeekParaOp::seek(tensorflow::OpKernelContext* ctx,
                         common::Request *request,
                         const rank::RankProfile *rankProfile,
                         SeekResource *seekResource,
                         service::SearcherResource *searcherResource)
{
    search::InnerSearchResult innerResult(seekResource->commonResource->pool);
    bool ret = processRequest(request, rankProfile, seekResource, searcherResource, innerResult);
    REQUEST_TRACE_WITH_TRACER(seekResource->commonResource->tracer, TRACE3,
                              "===== end parallel seek =====");
    tensorflow::Tensor* innerResultTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &innerResultTensor));
    InnerResultVariant innerResultVariant(innerResult);
    innerResultTensor->scalar<tensorflow::Variant>()() = innerResultVariant;
    tensorflow::Tensor* hasErrorTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &hasErrorTensor));
    hasErrorTensor->scalar<bool>()() = !ret;
}

bool Ha3SeekParaOp::processRequest(common::Request *request,
                                   const rank::RankProfile *rankProfile,
                                   SeekResource *seekResource,
                                   service::SearcherResource *searcherResource,
                                   search::InnerSearchResult &innerResult)
{
    auto &commonResource = seekResource->commonResource;
    auto &partitionResource = seekResource->partitionResource;
    auto &runtimeResource = seekResource->runtimeResource;
    auto &layerMetas = seekResource->layerMetas;
    if (!commonResource || !partitionResource || !runtimeResource) {
        return false;
    }
    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    auto optimizerChainManager = searcherResource->getOptimizerChainManager().get();
    auto sorterManager = searcherResource->getSorterManager().get();
    auto searcherCache = searcherResource->getSearcherCache().get();
    search::MatchDocSearcher searcher(*commonResource, *partitionResource,
            *runtimeResource, rankProfileMgr,
            optimizerChainManager, sorterManager,
            searcherResource->getAggSamplerConfigInfo(),
            searcherResource->getClusterConfig(),
            searcherCache, rankProfile,
            layerMetas);
    return searcher.seek(request, nullptr, innerResult);
}

AUTIL_LOG_SETUP(ha3, Ha3SeekParaOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SeekParaOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekParaOp);

} // namespace turing
} // namespace isearch
