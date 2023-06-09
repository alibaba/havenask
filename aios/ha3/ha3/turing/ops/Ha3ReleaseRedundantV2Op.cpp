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
#include "ha3/turing/ops/Ha3ReleaseRedundantV2Op.h"

#include <assert.h>
#include <stddef.h>
#include <algorithm>
#include <iostream>
#include <memory>
#include <memory>

#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/Request.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

void Ha3ReleaseRedundantV2Op::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    OP_REQUIRES(ctx, sessionResource,
                errors::Unavailable("session resource unavailable"));

    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherQueryResource,
                errors::Unavailable("ha3 searcher query resource unavailable"));

    SearchRuntimeResourcePtr runtimeResource = searcherQueryResource->runtimeResource;
    OP_REQUIRES(ctx, runtimeResource,
                errors::Unavailable("ha3 runtime resource unavailable"));

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto requestVariant = requestTensor.get<Ha3RequestVariant>();
    RequestPtr request = requestVariant->getRequest();

    auto requiredTopK = runtimeResource->docCountLimits.requiredTopK;
    auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
    auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
    common::Ha3MatchDocAllocatorPtr ha3MatchDocAllocator =
        dynamic_pointer_cast<common::Ha3MatchDocAllocator>(matchDocsVariant->getAllocator());
    OP_REQUIRES(ctx, ha3MatchDocAllocator,
                errors::Unavailable("dynamic_pointer_cast matchDocAllocator failed."));

    vector<matchdoc::MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);

    uint32_t resultCount = min((uint32_t)matchDocVec.size(), getResultCount(request.get(), requiredTopK, searcherQueryResource));
    releaseRedundantMatchDocs(matchDocVec, resultCount, ha3MatchDocAllocator);
    matchDocsVariant->stealMatchDocs(matchDocVec);
    Tensor* matchDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocs));
    matchDocs->scalar<Variant>()() = *matchDocsVariant;
}

void Ha3ReleaseRedundantV2Op::releaseRedundantMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVec,
                                   uint32_t resultCount,
                                   const common::Ha3MatchDocAllocatorPtr &matchDocAllocator)
{
    uint32_t size = (uint32_t)matchDocVec.size();
    while(size > resultCount)
    {
        auto matchDoc = matchDocVec[--size];
        matchDocAllocator->deallocate(matchDoc);
    }
    matchDocVec.erase(matchDocVec.begin() + size, matchDocVec.end());
}

uint32_t Ha3ReleaseRedundantV2Op::getResultCount(const Request *request, uint32_t requiredTopK, SearcherQueryResource *searcherQueryResource)
{
    SearcherCacheInfoPtr searchCacheInfo = searcherQueryResource->searcherCacheInfo;
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    if (!searchCacheInfo || searchCacheInfo->isHit || !cacheClause) {
        return requiredTopK;
    }

    const vector<uint32_t> &cacheDocNumVec = cacheClause->getCacheDocNumVec();
    uint32_t size = cacheDocNumVec.size();
    assert(size > 0);
    for (size_t i = 0; i < size; ++i) {
        if (cacheDocNumVec[i] >= requiredTopK) {
            return cacheDocNumVec[i];
        }
    }
    return requiredTopK;
}

AUTIL_LOG_SETUP(ha3, Ha3ReleaseRedundantV2Op);

REGISTER_KERNEL_BUILDER(Name("Ha3ReleaseRedundantV2Op")
                        .Device(DEVICE_CPU),
                        Ha3ReleaseRedundantV2Op);

} // namespace turing
} // namespace isearch
